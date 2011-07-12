"""Cryptographic operations for CC messages.

This implements PKCS#7/CMS crypto operations on plain (byte-string)
messages.  Signatures and encrypted messages are output as
binary DER-encoded messages.

Formatting messages for ZMQ packets is done in cc.message module.
"""

from M2Crypto import SMIME, BIO, X509
import os.path
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
        ca_crt = self.ks.load_cert_obj(ca_name)
        xk = X509.X509_Stack()
        xk.push(ca_crt)
        xs = X509.X509_Store()
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
    def __init__(self, cf, log):
        self.log = log
        if not cf:
            self.cms = None
            self.ks_dir = ''
            self.ks = KeyStore('', '')
            self.ca_name = None
            self.decrypt_name = None
            self.encrypt_name = None
            self.sign_name = None
            return
        self.ks_dir = cf.getfile('cms-keystore', '')
        priv_dir = os.path.join(self.ks_dir, 'private')
        ks = KeyStore(priv_dir, self.ks_dir)

        self.cms = CMSTool(ks)
        self.ca_name = cf.get('cms-verify-ca', '')
        self.decrypt_name = cf.get('cms-decrypt', '')
        self.sign_name = cf.get('cms-sign', '')
        self.encrypt_name = cf.get('cms-encrypt', '')

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

    def create_cmsg(self, msg):
        js = msg.dump_json()
        part1 = js
        part2 = ''
        if self.encrypt_name and self.sign_name:
            self.log.info("CryptoContext.create_cmsg: encrypt: %s", msg['req'])
            part1 = 'ENC1'
            part2 = self.cms.sign_and_encrypt(js, self.sign_name, self.encrypt_name)
        elif self.encrypt_name:
            raise Exception('encrypt_name without sign_name?')
        elif self.sign_name:
            self.log.info("CryptoContext.create_cmsg: sign: %s", msg['req'])
            part2 = self.cms.sign(js, self.sign_name)
        else:
            self.log.info("CryptoContext.create_cmsg: no crypto: %s", msg['req'])
        return CCMessage(['', msg.req.encode('utf8'), part1, part2])

    def parse_cmsg(self, cmsg):
        req = cmsg.get_dest()
        part1 = cmsg.get_part1()
        part2 = cmsg.get_part2()

        if self.decrypt_name:
            if part1 != 'ENC1':
                self.log.error('Expect encrypted message')
                return (None, None)
            if not self.decrypt_name or not self.ca_name:
                self.log.error('Cannot decrypt message')
                return (None, None)
            self.log.info("CryptoContext.parse_cmsg: decrypt: %s", cmsg.get_dest())
            js, sgn = self.cms.decrypt_and_verify(part2, self.decrypt_name, self.ca_name)
        elif part1 == 'ENC1':
            self.log.error('Got encrypted msg but cannot decrypt it')
            return (None, None)
        elif self.ca_name:
            if not part2:
                self.log.error('Expect signed message')
                return (None, None)
            self.log.info("CryptoContext.parse_cmsg: verify: %s", cmsg.get_dest())
            js, sgn = self.cms.verify(part1, part2, self.ca_name)
        else:
            self.log.info("CryptoContext.parse_cmsg: no crypto: %s", cmsg.get_dest())
            js, sgn = part1, None

        msg = Struct.from_json(js)
        if msg.req != req:
            self.log.error('hijacked message')
            return (None, None)

        return msg, sgn

#
# Test code follows
#

class TestStore(KeyStore):
    keys = {
        'ca.crt': """
        -----BEGIN CERTIFICATE-----
        MIIBoDCCAUoCCQCdAoPVECremzANBgkqhkiG9w0BAQUFADBXMQswCQYDVQQGEwJF
        RTEOMAwGA1UEBxMFVGFydHUxDjAMBgNVBAoTBVNreXBlMQ8wDQYDVQQLEwZUZW1w
        Q0ExFzAVBgNVBAMTDlRlc3QgQ0EgU2VydmVyMB4XDTExMDcwODEwMDcxNloXDTEx
        MDgwNzEwMDcxNlowVzELMAkGA1UEBhMCRUUxDjAMBgNVBAcTBVRhcnR1MQ4wDAYD
        VQQKEwVTa3lwZTEPMA0GA1UECxMGVGVtcENBMRcwFQYDVQQDEw5UZXN0IENBIFNl
        cnZlcjBcMA0GCSqGSIb3DQEBAQUAA0sAMEgCQQCnmcec9gxcCCLbwtSQBjLkqoWc
        v1HDnGMPshAo1CE//rGfRSriqgmNC7q8mgJgyFhJzi6pjwMkUkTnsfH/ytUBAgMB
        AAEwDQYJKoZIhvcNAQEFBQADQQA+he78x0tZOICxw6rb6RwkT1sqQsfnCsqKmSWd
        MxPMjPTcBWfdQsXyzV0Y3HFo79D132HsRBSW92qAtBs43hz1
        -----END CERTIFICATE-----
        """,
        'user1.key': """
        -----BEGIN RSA PRIVATE KEY-----
        MIIBOwIBAAJBALp8PH6ppiRr0WG8uAkbF4bxt1fm96kFjk85hW2RbtVxJAtneynT
        t5lZ9FxS5xofq3lwR/x7rFbLZ9WgyeBu+ikCAwEAAQJAXfvg7SEI55AjDTP0ODqc
        J9lIQpfXtypip1DhCvBhwFWRQqo1rwRIZibTaqIloXQkQUvdQE7TJpKEDZb4qRxb
        0QIhAOpg7c3INssh15e03/npSEtWFZGeL7s2Trw6USYUF3m9AiEAy7BC8Ey1nUoW
        h2Vghi+sNrrpcD7vvDmQNIXrNhPnit0CIFZtjeOva/02KpFH4rv+eWlGgkejZIiN
        uzUP8DKxgAKlAiEAgJ8FJgjKhlBKeaUilplz/ft5fU/AwvL2hLQsGzHmfGECIQCw
        b3RRHfiOEItOo5yw0qysme33h0feLUmwklSD/vN8Ww==
        -----END RSA PRIVATE KEY-----
        """,
        'user1.crt': """
        -----BEGIN CERTIFICATE-----
        MIIBtDCCAV4CAQEwDQYJKoZIhvcNAQEFBQAwVzELMAkGA1UEBhMCRUUxDjAMBgNV
        BAcTBVRhcnR1MQ4wDAYDVQQKEwVTa3lwZTEPMA0GA1UECxMGVGVtcENBMRcwFQYD
        VQQDEw5UZXN0IENBIFNlcnZlcjAeFw0xMTA3MDgxMDA3MTZaFw0zOTA3MjExMDA3
        MTZaMHMxCzAJBgNVBAYTAkVFMQ4wDAYDVQQHEwVUYXJ0dTEOMAwGA1UEChMFU2t5
        cGUxEDAOBgNVBAsTB1NpdGVPcHMxEjAQBgNVBAMTCVVzZXIgTmFtZTEeMBwGCSqG
        SIb3DQEJARYPdXNlcjFAc2t5cGUubmV0MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJB
        ALp8PH6ppiRr0WG8uAkbF4bxt1fm96kFjk85hW2RbtVxJAtneynTt5lZ9FxS5xof
        q3lwR/x7rFbLZ9WgyeBu+ikCAwEAATANBgkqhkiG9w0BAQUFAANBAFggPlcnT3m8
        sv/XCON+H/Owvoesae7+2VjivaDIYzd0e8a4ygj1biCF6L7qSY6qWFPtvK8k3HM4
        cEgTkF7ozFc=
        -----END CERTIFICATE-----
        """,
        'server.key': """
        -----BEGIN RSA PRIVATE KEY-----
        MIIBOwIBAAJBAOnQc5DT+q+kCVQEm3Usvb7JNknOhoEGAplFXIGVTogX1iigCl6F
        uUKRO6Fau9mvlOSxaW9FGMHmKOTesmHRYGECAwEAAQJAbOcvfKyPXdG8moqO0fPl
        6QAVLilokp33Bea9oImni1ES/jUb+gzYNmedCU51F05uOujGZ/Xn2q4uZcXq+JsK
        8QIhAPciRiPdM5Dq9hPAGfsW3i5e7nCFLaTDQJMJJrXmEMRlAiEA8jPY8m31NQD7
        kGcBlP5VRDGEu6LPwgS36M1EGCvlNk0CIA9ndoHDxvQQgTgn8DajbUPsrOYclwS/
        GuZPWrdZ2M+1AiEAnftru1Y154jojlxiD8mF3KFgLvQYCDoDq/qYPBwFutECIQCb
        7uDf4yD92IMH/gizerYsttbuY8TgcUKwJeal6r/Ysg==
        -----END RSA PRIVATE KEY-----
        """,
        'server.crt': """
        -----BEGIN CERTIFICATE-----
        MIIBkTCCATsCAQIwDQYJKoZIhvcNAQEFBQAwVzELMAkGA1UEBhMCRUUxDjAMBgNV
        BAcTBVRhcnR1MQ4wDAYDVQQKEwVTa3lwZTEPMA0GA1UECxMGVGVtcENBMRcwFQYD
        VQQDEw5UZXN0IENBIFNlcnZlcjAeFw0xMTA3MDgxMDA3MTZaFw0zOTA3MjExMDA3
        MTZaMFAxCzAJBgNVBAYTAkVFMQ4wDAYDVQQHEwVUYXJ0dTEOMAwGA1UEChMFU2t5
        cGUxEDAOBgNVBAsTB0R1YkNvbG8xDzANBgNVBAMTBkhvc3QgMTBcMA0GCSqGSIb3
        DQEBAQUAA0sAMEgCQQDp0HOQ0/qvpAlUBJt1LL2+yTZJzoaBBgKZRVyBlU6IF9Yo
        oApehblCkTuhWrvZr5TksWlvRRjB5ijk3rJh0WBhAgMBAAEwDQYJKoZIhvcNAQEF
        BQADQQBnerHVFKRD7LpI9wC7hhnGZKG7/PmWeuAdKbBDJDjwvysFf94NGRqT7U52
        eRCudduN9f4zFTC4V9gE6dY91+sD
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

