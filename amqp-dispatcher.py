#!/usr/bin/env python

__author__ = 'Georgios Gousios <gousiosg@gmail.com>'

import pika
import time
import sys
import os
import logging
import subprocess
import fcntl

from threading import Thread
from signal import signal, SIGINT, SIGTERM

log = logging.getLogger("process")
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(process)d - "
                              "%(levelname)s - %(message)s")
ch.setFormatter(formatter)
log.addHandler(ch)

# List of forked workers
children = []


class Worker:
    """
        A worker has a command line, connects to the AMQP queue and processes
         messages in a blocking fashion. For every new message, it forks a
         subprocess with the input command line and the message body appended
         at the end.
    """
    global log
    msgs = 0

    def __init__(self, opts, cmdline):
        log.debug("new worker with cmd-line %s" % cmdline)
        self.opts = opts
        self.cmdline = cmdline

    def start(self):
        """
            Main worker loop. Connects to AMQP and handles exceptions.
        """
        done = False
        sleep = 0.01
        while done is False:
            time.sleep(sleep)
            try:
                sleep += sleep
                credentials = pika.PlainCredentials(self.opts.queue_uname,
                                                    self.opts.queue_passwd)
                params = pika.ConnectionParameters(host=self.opts.queue_server,
                                                   credentials=credentials,
                                                   virtual_host="/")
                log.debug("Connecting to %s" % self.opts.queue_server)
                connection = pika.BlockingConnection(params)
                channel = connection.channel()
                channel.queue_declare(queue=self.opts.queue_name, durable=True)
                channel.basic_qos(prefetch_count=1)
                channel.basic_consume(self.on_message, self.opts.queue_name)
                channel.start_consuming()
            except SystemExit:
                log.info("System exit caught, exiting")
                channel.stop_consuming()
                connection.close()
                done = True
            except Exception:
                log.exception("Could not connect to %s" %
                              self.opts.queue_server)
                if channel.is_open():
                    channel.close()
                if connection.is_open():
                    connection.close()

    def on_message(self, channel, method_frame, header_frame, body):
        """
            On every incoming message: fork a new subprocess with the assigned
            command line and the incoming message body appended to it. Also
            redirects stdin and stderr to log.info and log.warn respectively.
        """
        try:
            log.debug("worker %d got message %s" % (os.getpid(), body))
            def logger_worker(logger, type, stream):
                fd = stream.fileno()
                fl = fcntl.fcntl(fd, fcntl.F_GETFL)
                fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
                try:
                    msg = stream.read()
                except:
                    msg = ''

                if msg.strip():
                    if type == "WARN":
                        log.warn("%s" % msg)
                    elif type == "INFO":
                        log.info("%s" % msg)
                    else:
                        print msg

            process = subprocess.Popen([self.cmdline, body],
                stdin=sys.stdin,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)

            stdout_logger = Thread(target=logger_worker,
                                   args=[log, "INFO", process.stdout])
            stdout_logger.daemon = True
            stdout_logger.start()
            stderr_logger = Thread(target=logger_worker,
                                   args=[log, "WARN", process.stderr])
            stderr_logger.daemon = True
            stderr_logger.start()

            process.wait()
            stdout_logger.join(timeout=1)
            stderr_logger.join(timeout=1)

            if process.returncode is not 0:
                log.warn("Process: %s returned code :%d" %
                         (" ".join([self.cmdline, body]), process.returncode))

            self.msgs += 1
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        except OSError:
            channel.basic_reject(delivery_tag=method_frame.delivery_tag, requeue=False)
            log.exception("Error spawing process: %s for msg: %s" % (self.cmdline, body))
        except Exception:
            channel.basic_reject(delivery_tag=method_frame.delivery_tag,
                                 requeue=(not method_frame.redelivered))
            log.exception("exception processing message: %s" % body)


def parse_arguments(args):
    from argparse import ArgumentParser, REMAINDER

    parser = ArgumentParser(description="Dispatch AMQP msgs to a list of workers")
    parser.add_argument("-d", "--debug", action="store_true", default=False,
                        dest="debug", help="Enable debug mode")

    # Queue connection info
    parser.add_argument("-s", "--queue-server", required=True,
                        default="localhost", dest="queue_server",
                        help="Queue server to connect to")
    parser.add_argument("-u", "--queue-username", required=True, default="",
                        dest="queue_uname",
                        help="Username to connect to the queue")
    parser.add_argument("-p", "--queue-password", required=True, default="",
                        dest="queue_passwd",
                        help="Password to connect to the queue")
    parser.add_argument("-q", "--queue-name", required=True, default="",
                        dest="queue_name", help="Queue name to listen to")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-w", "--num-workers", dest="workers",
                       help="Workers to spawn", type=int)
    group.add_argument("-f", "--workers-file",
                       dest="workers_file", help="Workers to spawn")

    parser.add_argument('cmd', nargs=REMAINDER,
                        help='Worker command (ignored if -f is used)')
    return parser.parse_args(args)


def _exit_handler(signum, frame):
    """"Catch exit signal in children processes"""
    global log
    log.info("Caught signal %d, will raise SystemExit", signum)
    raise SystemExit


def _parent_handler(signum):
    """"Catch exit signal in parent process and forward it to children."""
    global children

    log.info("Caught signal %d, sending SIGTERM to children %s", signum, children)
    [os.kill(pid, SIGTERM) for pid in children]


def spawn_workers(opts, cmd_lines):
    global children

    log.info("Spawning %s workers" % opts.workers)
    # Fork workers
    children = []
    i = 0

    for cmd in cmd_lines:
        try:
            newpid = os.fork()
            if newpid == 0:
                signal(SIGINT, _exit_handler)
                signal(SIGTERM, _exit_handler)
                Worker(opts, cmd).start()
                sys.exit(1)
            else:
                log.debug("%d, forked child: %d", os.getpid(), newpid)
                children.append(newpid)
        except Exception:
            log.exception("Error spawning worker %d" % i)

    # Catch signals to ensure graceful shutdown
    signal(SIGINT, _parent_handler)
    signal(SIGTERM, _parent_handler)

    # Wait for all children processes to die, one by one
    for pid in children:
        try:
            os.waitpid(pid, 0)
        except Exception:
            pass


def main():

    opts = parse_arguments(sys.argv[1:])
    cmd_lines = []
    if opts.workers_file is None:
        if not opts.cmd:
            exit("cmd argument is required with -w option")
        for i in range(opts.workers):
            cmd_lines.append(" ".join(opts.cmd))
    else:
        f = open(opts.workers_file, "r")
        for line in f:
            if line.strip():
                cmd_lines.append(line.rstrip())
        f.close

    try:
        spawn_workers(opts, cmd_lines)
        return 1
    except Exception:
        log.exception("Unknown error")
        return 0


if __name__ == "__main__":
    sys.exit(main())
