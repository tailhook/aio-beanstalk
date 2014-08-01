from .packets import BasePacket


class PutJobError(Exception):
    """Error when putting job in queue"""


class Buried(PutJobError, BasePacket):
    """Not enough resources to get the job"""
    token = "BURIED"
    fields = [('job_id', int)]

    def __init__(self, job_id):
        self.job_id = job_id
        return super().__init__(job_id)

    def __str__(self):
        return "Job was buried. (job-id: {!r})".format(self.job_id)


class JobTooBig(PutJobError, BasePacket):
    """Job is larger than server accepts"""
    token = "JOB_TOO_BIG"


class Draining(PutJobError, BasePacket):
    """The server is in Drain Mode"""
    token = "DRAINING"


class DeadlineSoon(Exception, BasePacket):
    """Deadline is soon. What we can do?"""
    token = "DEADLINE_SOON"


class TimedOut(Exception, BasePacket):
    """Reserve command timed out (No pending tasks)"""
    token = "TIMED_OUT"


class NotFound(Exception, BasePacket):
    """Job not found"""
    token = "NOT_FOUND"


class OutOfMemory(Exception, BasePacket):
    """Server is in out of memory state"""
    token = "OUT_OF_MEMORY"


class InternalError(Exception, BasePacket):
    """Internal Server Error"""
    token = "INTERNAL_ERROR"


class BadFormat(Exception, BasePacket):
    """Internal Server Error"""
    token = "BAD_FORMAT"


class UnknownCommand(Exception, BasePacket):
    """Unknown command send to server"""
    token = "UNKNOWN_COMMAND"


class NotIgnored(Exception, BasePacket):
    """The only wathched tube can't be ignored"""
    token = "NOT_IGNORED"
