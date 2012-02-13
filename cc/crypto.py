"""Cryptographic operations for CC messages.

This implements PKCS#7/CMS crypto operations on plain (byte-string)
messages.  Signatures and encrypted messages are output as
binary DER-encoded messages.

Formatting messages for ZMQ packets is done in cc.message module.
"""

import os.path
import time

import skytools

from hashlib import sha1
from M2Crypto import SMIME, BIO, X509

from cc.json import Struct
from cc.message import CCMessage

# loading base64 msg is horribly slow (openssl:PEM_read_bio_PKCS7)
# switch to binary DER encoding instead
RAW_MESSAGES = 1

# re-use initialized context
CACHE_KEYS = 1

# M2Crypto forgot to provide helper function for DER msgs
from M2Crypto import m2, Err
def load_pkcs7_bio_der(p7_bio):
    p7_ptr = m2.pkcs7_read_bio_der(p7_bio._ptr())
    if p7_ptr is None:
        raise SMIME.PKCS7_Error(Err.get_error())
    return SMIME.PKCS7(p7_ptr, 1)


class KeyStore(object):
    """Keys and certs in separate dirs, different extensions.

    Expects PEM-formatted storage.
    """
    def __init__(self, priv_dir, cert_dir, key_ext = '.key', cert_ext = '.crt'):
        self.priv_dir = priv_dir
        self.cert_dir = cert_dir
        self.key_ext = key_ext
        self.cert_ext = cert_ext

    def load_key(self, keyname):
        """Load key as bytes"""
        if keyname.find('/') >= 0:
            raise Exception('invalid key name')
        fn = os.path.join(self.priv_dir, keyname + self.key_ext)
        return self.read_file(fn)

    def load_cert(self, certname):
        """Load cert as bytes"""
        if certname.find('/') >= 0:
            raise Exception('invalid cert name')
        fn = os.path.join(self.cert_dir, certname + self.cert_ext)
        return self.read_file(fn)

    def load_key_bio(self, keyname):
        """Load key as bio"""
        data = self.load_key(keyname)
        return BIO.MemoryBuffer(data)

    def load_cert_bio(self, certname):
        """Load cert as bio"""
        data = self.load_cert(certname)
        return BIO.MemoryBuffer(data)

    def load_cert_obj(self, certname):
        """Load cert as X509 object"""
        bio = self.load_cert_bio(certname)
        return X509.load_cert_bio(bio)

    def read_file(self, fn):
        """Properly read a file"""
        f = open(fn, 'rb')
        data = f.read()
        f.close()
        return data


class CMSTool:
    """Cryptographic Message Syntax

    We use SMIME algorithms, but not formatting.   Instead
    the data is formatted as simple PKCS7 blobs.
    """
    def __init__(self, keystore):
        self.ks = keystore
        self.cache = {}

    def sign(self, data, sender_name, detached=True):
        """Create detached signature.

        Requires both private key and it's cert.

        Returns signature.
        """

        sm = self.get_sign_ctx(sender_name)

        # sign
        bdata = BIO.MemoryBuffer(data)
        flags = SMIME.PKCS7_BINARY
        if detached:
            flags |= SMIME.PKCS7_DETACHED
        pk = sm.sign(bdata, flags)

        # return signature
        res = BIO.MemoryBuffer()
        if RAW_MESSAGES:
            pk.write_der(res)
        else:
            pk.write(res)
        return res.read()

    def verify(self, data, signature, ca_name, detached=True):
        """Verify detached SMIME signature.

        Requires CA cert.

        Returns tuple (data, signer_details_dict).
        """

        sm = self.get_verify_ctx(ca_name)

        # init data
        bsign = BIO.MemoryBuffer(signature)
        if RAW_MESSAGES:
            pk = load_pkcs7_bio_der(bsign)
        else:
            pk = SMIME.load_pkcs7_bio(bsign)

        # check signature
        if detached:
            bdata = BIO.MemoryBuffer(data)
            data2 = sm.verify(pk, bdata, flags = SMIME.PKCS7_BINARY | SMIME.PKCS7_DETACHED)
            assert data2 == data
        elif data:
            raise Exception('Have data when detached=False')
        else:
            data2 = sm.verify(pk, flags = SMIME.PKCS7_BINARY)

        return data2, self.get_signature_info(sm, pk)

    def encrypt(self, plaintext, receiver_name):
        """Encrypt message.

        Requires recipient cert.

        Returns encrypted message.
        """

        sm = self.get_encrypt_ctx(receiver_name)

        # encrypt data
        bdata = BIO.MemoryBuffer(plaintext)
        pk = sm.encrypt(bdata, SMIME.PKCS7_BINARY)

        # return ciphertext
        buf = BIO.MemoryBuffer()
        if RAW_MESSAGES:
            pk.write_der(buf)
        else:
            pk.write(buf)
        return buf.read()

    def decrypt(self, ciphtext, receiver_name):
        """Decrypt message.

        Requires private key and it's cert.

        Returns decrypted message.
        """

        sm = self.get_decrypt_ctx(receiver_name)

        # decrypt
        bdata = BIO.MemoryBuffer(ciphtext)
        if RAW_MESSAGES:
            pk = load_pkcs7_bio_der(bdata)
        else:
            pk = SMIME.load_pkcs7_bio(bdata)
        return sm.decrypt(pk, SMIME.PKCS7_BINARY)

    def sign_and_encrypt(self, data, sender_name, receiver_name):
        """Embed data and signature in encrypted message.

        Requires sender's private key+cert and receivers cert.

        Returns encrypted message.
        """

        msg = self.sign(data, sender_name, detached=False)
        return self.encrypt(msg, receiver_name)

    def decrypt_and_verify(self, ciphertext, receiver_name, ca_name):
        """Decrypt and check signature of embedded message.

        Requires receivers's private key+cert and CA cert.

        Returns tuple: (plaintext, signer_details_dict)
        """
        body = self.decrypt(ciphertext, receiver_name)
        return self.verify(None, body, ca_name, detached=False)

    def get_signature_info(self, sm, pk):
        """Returns dict of info fields."""

        sign_stack = pk.get0_signers(sm.x509_stack)
        crt0 = sign_stack.pop()
        inf = {}
        inf['not_before'] = crt0.get_not_before().get_datetime()
        inf['not_after'] = crt0.get_not_after().get_datetime()
        inf['serial'] = crt0.get_serial_number()
        inf['version'] = crt0.get_version()
        subj = crt0.get_subject()
        inf['subject'] = subj.as_text()
        for k in ['C', 'ST', 'CN', 'O', 'L', 'emailAddress']:
            if hasattr(subj, k):
                inf[k] = getattr(subj, k)

        # disallow multiple certs in signature - dunno when it happens
        if sign_stack.pop():
            raise Exception('Confused by multiple certs in signature')

        return inf

    def get_sign_ctx(self, sender_name):
        """Return SMIME context for signing."""

        ck = ('S', sender_name)
        if ck in self.cache:
            return self.cache[ck]

        # setup signature key
        sm = SMIME.SMIME()
        sm.load_key_bio(
                self.ks.load_key_bio(sender_name),
                self.ks.load_cert_bio(sender_name))

        if CACHE_KEYS:
            self.cache[ck] = sm
        return sm

    def get_verify_ctx(self, ca_name):
        """Return SMIME context for verification."""

        ck = ('V', ca_name)
        if ck in self.cache:
            return self.cache[ck]

        # load ca cert(s)
        xk = X509.X509_Stack()
        xs = X509.X509_Store()
        for cn in ca_name.split(','):
            cn = cn.strip()
            ca_crt = self.ks.load_cert_obj(cn)
            xk.push(ca_crt)
            xs.add_cert(ca_crt)

        # init SMIME object
        sm = SMIME.SMIME()
        sm.set_x509_stack(xk)
        sm.set_x509_store(xs)

        if CACHE_KEYS:
            self.cache[ck] = sm
        return sm

    def get_encrypt_ctx(self, receiver_name):
        """Return SMIME context for encryption."""

        ck = ('E', receiver_name)
        if ck in self.cache:
            return self.cache[ck]

        # here we could add several certs
        crt = self.ks.load_cert_obj(receiver_name)
        x = X509.X509_Stack()
        x.push(crt)

        # SMIME setup
        sm = SMIME.SMIME()
        sm.set_x509_stack(x)
        sm.set_cipher(SMIME.Cipher('aes_128_cbc'))

        if CACHE_KEYS:
            self.cache[ck] = sm
        return sm

    def get_decrypt_ctx(self, receiver_name):
        """Return SMIME context for decryption."""

        ck = ('D', receiver_name)
        if ck in self.cache:
            return self.cache[ck]

        # load key
        sm = SMIME.SMIME()
        sm.load_key_bio(
                self.ks.load_key_bio(receiver_name),
                self.ks.load_cert_bio(receiver_name))

        if CACHE_KEYS:
            self.cache[ck] = sm
        return sm


#
# CC-specific crypto conf
#

class CryptoContext:
    """Load crypto config, check messages based on it."""

    log = skytools.getLogger('CryptoContext')

    def __init__(self, cf):
        if not cf:
            self.cms = None
            self.ks_dir = ''
            self.ks = KeyStore('', '')
            self.ca_name = None
            self.decrypt_name = None
            self.encrypt_name = None
            self.sign_name = None
            self.time_window = 0
            return
        self.ks_dir = cf.getfile('cms-keystore', '')
        priv_dir = os.path.join(self.ks_dir, 'private')
        ks = KeyStore(priv_dir, self.ks_dir)

        self.cms = CMSTool(ks)
        self.ca_name = cf.get('cms-verify-ca', '')
        self.decrypt_name = cf.get('cms-decrypt', '')
        self.sign_name = cf.get('cms-sign', '')
        self.encrypt_name = cf.get('cms-encrypt', '')
        self.time_window = int(cf.get('cms-time-window', '0'))

    def fill_config(self, cf_dict):
        pairs = (('cms-verify-ca', 'ca_name'),
                 ('cms-decrypt', 'decrypt_name'),
                 ('cms-sign', 'sign_name'),
                 ('cms-encrypt', 'encrypt_name'),
                 ('cms-keystore', 'ks_dir'))
        for n1, n2 in pairs:
            v = getattr(self, n2)
            if v and n1 not in cf_dict:
                cf_dict[n1] = v

    def create_cmsg(self, msg, blob=None):
        if blob is not None and self.sign_name:
            msg.blob_hash = "SHA-1:" + sha1(blob).hexdigest()
        js = msg.dump_json()
        part1 = js
        part2 = ''
        if self.encrypt_name and self.sign_name:
            self.log.trace("encrypt: %s", msg['req'])
            part1 = 'ENC1'
            part2 = self.cms.sign_and_encrypt(js, self.sign_name, self.encrypt_name)
        elif self.encrypt_name:
            raise Exception('encrypt_name without sign_name ?')
        elif self.sign_name:
            self.log.trace("sign: %s", msg['req'])
            part2 = self.cms.sign(js, self.sign_name)
        else:
            self.log.trace("no crypto: %s", msg['req'])
        zmsg = ['', msg.req.encode('utf8'), part1, part2]
        if blob is not None:
            zmsg.append(blob)
        return CCMessage(zmsg)

    def parse_cmsg(self, cmsg):
        req = cmsg.get_dest()
        part1 = cmsg.get_part1()
        part2 = cmsg.get_part2()
        blob = cmsg.get_part3()

        if self.decrypt_name:
            if part1 != 'ENC1':
                self.log.error('Expect encrypted message')
                return (None, None)
            if not self.decrypt_name or not self.ca_name:
                self.log.error('Cannot decrypt message')
                return (None, None)
            self.log.trace("decrypt: %s", cmsg.get_dest())
            js, sgn = self.cms.decrypt_and_verify(part2, self.decrypt_name, self.ca_name)
        elif part1 == 'ENC1':
            self.log.error('Got encrypted msg but cannot decrypt it')
            return (None, None)
        elif self.ca_name:
            if not part2:
                self.log.error('Expect signed message: %r', part1)
                return (None, None)
            self.log.trace("verify: %s", cmsg.get_dest())
            js, sgn = self.cms.verify(part1, part2, self.ca_name)
        else:
            self.log.trace("no crypto: %s", cmsg.get_dest())
            js, sgn = part1, None

        msg = Struct.from_json(js)
        if msg.req != req:
            self.log.error ('hijacked message')
            return (None, None)

        if self.time_window:
            age = time.time() - msg.time
            if abs(age) > self.time_window:
                self.log.error('time diff bigger than %d s', self.time_window)
                return (None, None)

        if blob is not None:
            if not self.ca_name and not part2:
                if getattr(msg, 'blob_hash', None):
                    self.log.debug ('blob hash ignored')
            elif getattr(msg, 'blob_hash', None):
                ht, hs, hv = msg.blob_hash.partition(':')
                if ht == 'SHA-1':
                    bh = sha1(blob).hexdigest()
                else:
                    self.log.error ('unsupported hash type: %s', ht)
                    return (None, None)
                if bh != hv:
                    self.log.error ('blob hash does not match: %s <> %s', bh, hv)
                    return (None, None)
            else:
                self.log.error ('blob hash missing')
                return (None, None)
        elif msg.get('blob_hash', None):
            self.log.error ('blob hash exists without blob')
            return (None, None)
        return msg, sgn

#
# Test code follows
#

class TestStore(KeyStore):
    keys = {
        'ca.crt': """
        -----BEGIN CERTIFICATE-----
        MIIBoDCCAUoCCQCFMOUR1Q3vxjANBgkqhkiG9w0BAQUFADBXMQswCQYDVQQGEwJF
        RTEOMAwGA1UEBxMFVGFydHUxDjAMBgNVBAoTBVNreXBlMQ8wDQYDVQQLEwZUZW1w
        Q0ExFzAVBgNVBAMTDlRlc3QgQ0EgU2VydmVyMB4XDTExMDgxOTEyMzI1N1oXDTM5
        MDkwMTEyMzI1N1owVzELMAkGA1UEBhMCRUUxDjAMBgNVBAcTBVRhcnR1MQ4wDAYD
        VQQKEwVTa3lwZTEPMA0GA1UECxMGVGVtcENBMRcwFQYDVQQDEw5UZXN0IENBIFNl
        cnZlcjBcMA0GCSqGSIb3DQEBAQUAA0sAMEgCQQCxEGMSvtvdM5Rw0ld2gSaYmn9b
        xEWQ+ak7ZicaBHyxYeMtqZs1EyEux14ghoaLc9kkETSwGjaLllxg0NUjzUBXAgMB
        AAEwDQYJKoZIhvcNAQEFBQADQQBY5AfjmMBu1kL3T40HJDPd57xV2IBh9k0YR4XK
        0GWgXplpDGqpWF00pNxPUOrbFwIGXT3vW+QLf2Woa8l8baqi
        -----END CERTIFICATE-----
        """,
        'user1.key': """
        -----BEGIN RSA PRIVATE KEY-----
        MIIBOgIBAAJBAMqc396gInfXcvoBnzS1Z2vvCSrdXU0mXkaOp2qWFpOBa4XAXCW/
        S7gLHJxzEa921w1aZDYSQ2BfWBVt3AnF61UCAwEAAQJAUFG3/Y0FnPRvw+P4tPBk
        u0jbHX77iaX4IYhTndE5yecDQ4tFZOQKase5mRGXgjALh4bkhFuiVBytkh6Kaori
        UQIhAPLhbVgDCzDyqJ/iP8Z4dfGTpTm1kuQxu5Me59ztHaWbAiEA1Y6e/7gws7Zs
        45euTVhuiCUjhwQkVhRwRuLu39hguc8CIQDKBZfdzSpR3IVF9/r0Kt5vwk98YPt9
        s6BCD0LtEI3IYQIgCeLyZXBHgOpfHCI3hYkkhNUDUgrVC88ia4Wx/VbtE20CIBtY
        ymZpQVFy2YkFzY9wsuz5DvfbIxmcuESXFgFtfuRm
        -----END RSA PRIVATE KEY-----
        """,
        'user1.crt': """
        -----BEGIN CERTIFICATE-----
        MIIBtDCCAV4CAQEwDQYJKoZIhvcNAQEFBQAwVzELMAkGA1UEBhMCRUUxDjAMBgNV
        BAcTBVRhcnR1MQ4wDAYDVQQKEwVTa3lwZTEPMA0GA1UECxMGVGVtcENBMRcwFQYD
        VQQDEw5UZXN0IENBIFNlcnZlcjAeFw0xMTA4MTkxMjMyNTdaFw0zOTA5MDExMjMy
        NTdaMHMxCzAJBgNVBAYTAkVFMQ4wDAYDVQQHEwVUYXJ0dTEOMAwGA1UEChMFU2t5
        cGUxEDAOBgNVBAsTB1NpdGVPcHMxEjAQBgNVBAMTCVVzZXIgTmFtZTEeMBwGCSqG
        SIb3DQEJARYPdXNlcjFAc2t5cGUubmV0MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJB
        AMqc396gInfXcvoBnzS1Z2vvCSrdXU0mXkaOp2qWFpOBa4XAXCW/S7gLHJxzEa92
        1w1aZDYSQ2BfWBVt3AnF61UCAwEAATANBgkqhkiG9w0BAQUFAANBAG3DyYjXjlCu
        9BYmt/5NyxHWM0xyIXZXzNEFOWRGyw4e7WjIzldrZfDXm5EMInPPskOCbOoXYqwA
        vtbi92yp0l0=
        -----END CERTIFICATE-----
        """,
        'server.key': """
        -----BEGIN RSA PRIVATE KEY-----
        MIIBOwIBAAJBAN9uPvmhTCSe4Wev+IDT2TL75N4dkn9QcTbLOaXyr1bntBFMcil9
        7j92D0CDwgV/UajhCz1OeHdTLJD85eLsgq8CAwEAAQJAaf/vuJahfS4zWfHOP7BB
        90IyDn6RJf2P+KLpsqU0MlHPtr2fqAEMqYoWueEnOaePA10aU5ywv0/w8CP3fXGA
        AQIhAPen1MSKteA/dWvhZhQB+KZQbFuZCTUAKWzOMiReX3KvAiEA5vV2C2/xWV2l
        Ro/1dSVLP/zlmsnKg317GGMTrQ3H8AECIQCWPXy4VyYLCrRjY/QXQzLjQnrZ/rc1
        Lgnzdgu5QH9LBQIhAII524Kdbw+9rsh3uaaBDcoZtfkuWOMFaNgaXWjRgXABAiAf
        LmnSar3/SeOBfPBhHuf+le/Al4dfFYdbyxum/Ormbw==
        -----END RSA PRIVATE KEY-----
        """,
        'server.crt': """
        -----BEGIN CERTIFICATE-----
        MIIBkTCCATsCAQIwDQYJKoZIhvcNAQEFBQAwVzELMAkGA1UEBhMCRUUxDjAMBgNV
        BAcTBVRhcnR1MQ4wDAYDVQQKEwVTa3lwZTEPMA0GA1UECxMGVGVtcENBMRcwFQYD
        VQQDEw5UZXN0IENBIFNlcnZlcjAeFw0xMTA4MTkxMjMyNTdaFw0zOTA5MDExMjMy
        NTdaMFAxCzAJBgNVBAYTAkVFMQ4wDAYDVQQHEwVUYXJ0dTEOMAwGA1UEChMFU2t5
        cGUxEDAOBgNVBAsTB0R1YkNvbG8xDzANBgNVBAMTBkhvc3QgMTBcMA0GCSqGSIb3
        DQEBAQUAA0sAMEgCQQDfbj75oUwknuFnr/iA09ky++TeHZJ/UHE2yzml8q9W57QR
        THIpfe4/dg9Ag8IFf1Go4Qs9Tnh3UyyQ/OXi7IKvAgMBAAEwDQYJKoZIhvcNAQEF
        BQADQQBR8As4fbUtHBPbQnwf1f0TmiZITG0O+Wz0DbX7WHuux3u+cuU3Q1GHXn9C
        DSbhOH3B/QPxFiFZA9cLdZQoy0L1
        -----END CERTIFICATE-----
        """
    }
    def __init__(self):
        super(TestStore, self).__init__('.', '.')

    def read_file(self, fn):
        fn = os.path.basename(fn)
        return self.keys[fn].replace(' '*8, '')


def test():
    msg = """{ "foo": "baaaaaaaaaaaaaaaaaaaaaaaaaaaaaar" }\n"""

    # larger message to check if detachment works
    msg = msg * 100

    c = CMSTool(TestStore())
    #c = CMSTool(KeyStore('./keys', './keys'))

    print 'Signing...'
    sgn = c.sign(msg, 'user1')
    assert len(sgn) < len(msg)
    #print sgn
    print 'Checking...'
    c.verify(msg, sgn, 'ca')
    print 'OK'

    print 'Encrypting...'
    enc = c.encrypt(msg, 'user1')
    #print enc
    print len(enc)
    assert enc != msg
    assert len(enc) > len(msg)

    print 'Decrypting...'
    txt = c.decrypt(enc, 'user1')
    #print txt
    assert txt == msg
    print 'OK'


def bench():
    msg = """{ "foo": "baaaaaaaaaaaaaaaaaaaaaaaaaaaaaar" }\n"""
    msg = msg * 50
    c = CMSTool(TestStore())
    #c = CMSTool(KeyStore('./keys', './keys'))
    import time

    print 'msg len', len(msg)

    count = 5000

    print 'Signing...'

    start = time.time()
    for i in range(count):
        sgn = c.sign(msg, 'user1')
    now = time.time()
    if count > 1 and now > start:
        print 'rate', count / (0.0 + now - start)

    print 'Checking...'
    start = time.time()
    for i in range(count):
        c.verify(msg, sgn, 'ca')
    now = time.time()
    if count > 1 and now > start:
        print 'rate', count / (0.0 + now - start)

    print 'Encrypting...'
    start = time.time()
    for i in range(count):
        enc = c.encrypt(msg, 'user1')
    now = time.time()
    if count > 1 and now > start:
        print 'rate', count / (0.0 + now - start)

    print 'Decrypting...'
    start = time.time()
    for i in range(count):
        txt = c.decrypt(enc, 'user1')
    now = time.time()
    if count > 1 and now > start:
        print 'rate', count / (0.0 + now - start)

    print 'sign_and_encrypt'
    start = time.time()
    for i in range(count):
        enc = c.sign_and_encrypt(msg, 'user1', 'server')
    now = time.time()
    if count > 1 and now > start:
        print 'rate', count / (0.0 + now - start)

    print 'decrypt_and_verify'
    start = time.time()
    for i in range(count):
        msg2, inf = c.decrypt_and_verify(enc, 'server', 'ca')
    now = time.time()
    if count > 1 and now > start:
        print 'rate', count / (0.0 + now - start)

    print 'OK'


if __name__ == '__main__':
    test()
    bench()
