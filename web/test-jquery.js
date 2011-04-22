var AjaxUrl = "/confdb/ajax";

var sess_user = "anonymous";
var sess_id = "";

function logmsg(msg)
{
    $("#log").append("LOG: " + msg + "<br>\n");
}

function got_answer(data)
{
    logmsg("Server: req=" + data.req + ": " + data.msg);

    if (data.req == 'poll') {
	next_poll();
    } else if (data.req == 'start') {
    } else if (data.req == 'login') {
	logmsg('session id: ' + data.session_id);
	sess_id = data.session_id;
	next_poll();
    }
}

function next_poll()
{
    $.getJSON(AjaxUrl, {
		'req': 'poll',
		'user': sess_user,
		'session_id': sess_id
	    }, got_answer);
}

function got_start(event)
{
    logmsg("Start button clicked, sending task");
    $.getJSON(AjaxUrl, {
		'req': 'start',
		'user': sess_user,
		'session_id': sess_id,
		'task': 'sometask'
	    }, got_answer);
    event.preventDefault();
}

function launcher()
{
    $.ajaxSetup({cache: false, type: 'POST'});
    $("#startButton").click(got_start);
    logmsg("JQuery launched");
    logmsg("Logging in...");
    $.getJSON(AjaxUrl, {
		'req': 'login',
		'user': sess_user
	    }, got_answer);
}

$(document).ready(launcher);

