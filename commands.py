"""
distutils commands for riak-python-test
"""

__all__ = ['create_bucket_types', 'preconfigure', 'configure']

from distutils import log
from distutils.core import Command
from distutils.errors import DistutilsOptionError
from subprocess import Popen, PIPE
from string import Template
import shutil
import re
import os.path


# Exception classes used by this module.
class CalledProcessError(Exception):
    """This exception is raised when a process run by check_call() or
    check_output() returns a non-zero exit status.
    The exit status will be stored in the returncode attribute;
    check_output() will also store the output in the output attribute.
    """
    def __init__(self, returncode, cmd, output=None):
        self.returncode = returncode
        self.cmd = cmd
        self.output = output

    def __str__(self):
        return "Command '%s' returned non-zero exit status %d" % (self.cmd,
                                                                  self
                                                                  .returncode)


def check_output(*popenargs, **kwargs):
    """Run command with arguments and return its output as a byte string.

    If the exit code was non-zero it raises a CalledProcessError.  The
    CalledProcessError object will have the return code in the returncode
    attribute and output in the output attribute.

    The arguments are the same as for the Popen constructor.  Example:

    >>> check_output(["ls", "-l", "/dev/null"])
    'crw-rw-rw- 1 root root 1, 3 Oct 18  2007 /dev/null\n'

    The stdout argument is not allowed as it is used internally.
    To capture standard error in the result, use stderr=STDOUT.

    >>> import sys
    >>> check_output(["/bin/sh", "-c",
    ...               "ls -l non_existent_file ; exit 0"],
    ...              stderr=sys.stdout)
    'ls: non_existent_file: No such file or directory\n'
    """
    if 'stdout' in kwargs:
        raise ValueError('stdout argument not allowed, it will be '
                         'overridden.')
    process = Popen(stdout=PIPE, *popenargs, **kwargs)
    output, unused_err = process.communicate()
    retcode = process.poll()
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        raise CalledProcessError(retcode, cmd, output=output)
    return output

try:
    import simplejson as json
except ImportError:
    import json


class create_bucket_types(Command):
    """
    Creates bucket-types appropriate for testing. By default this will create:

    * `twitter`
    """

    description = "create bucket-types used in integration tests"

    user_options = [
        ('riak-admin=', None, 'path to the riak-admin script')
    ]

    _props = {
        'twitter': {},
    }

    def initialize_options(self):
        self.riak_admin = None

    def finalize_options(self):
        if self.riak_admin is None:
            raise DistutilsOptionError("riak-admin option not set")

    def run(self):
        if self._check_available():
            for name in self._props:
                self._create_and_activate_type(name, self._props[name])

    def check_output(self, *args, **kwargs):
        if self.dry_run:
            log.info(' '.join(args))
            return bytearray()
        else:
            return check_output(*args, **kwargs)

    def _check_available(self):
        try:
            self.check_btype_command("list")
            return True
        except CalledProcessError:
            log.error("Bucket types are not supported on this Riak node!")
            return False

    def _create_and_activate_type(self, name, props):
        # Check status of bucket-type
        exists = False
        active = False
        try:
            status = self.check_btype_command('status', name)
        except CalledProcessError as e:
            status = e.output

        exists = ('not an existing bucket type' not in status)
        active = ('is active' in status)

        if exists or active:
            log.info("Updating {0} bucket-type with props {1}"
                     .format(repr(name), repr(props)))
            self.check_btype_command("update", name,
                                     json.dumps({'props': props},
                                                separators=(',', ':')))
        else:
            print name
            print props
            log.info("Creating {0} bucket-type with props {1}"
                     .format(repr(name), repr(props)))
            self.check_btype_command("create", name,
                                     json.dumps({'props': props},
                                                separators=(',', ':')))

        if not active:
            log.info('Activating {0} bucket-type'.format(repr(name)))
            self.check_btype_command("activate", name)

    def check_btype_command(self, *args):
        cmd = self._btype_command(*args)
        return self.check_output(cmd)

    def run_btype_command(self, *args):
        self.spawn(self._btype_command(*args))

    def _btype_command(self, *args):
        cmd = [self.riak_admin, "bucket-type"]
        cmd.extend(args)
        return cmd


class preconfigure(Command):
    """
    Sets up security configuration.

    * Update these lines in riak.conf
        * storage_backend = leveldb
        * search = on
        * listener.protobuf.internal = 127.0.0.1:8087
        * listener.http.internal = 127.0.0.1:8098
        * listener.https.internal = 127.0.0.1:18098
        * check_crl = off
    """

    description = "preconfigure security settings used in integration tests"

    user_options = [
        ('riak-conf=', None, 'path to the riak.conf file'),
        ('host=', None, 'IP of host running Riak'),
        ('pb-port=', None, 'protocol buffers port number'),
        ('https-port=', None, 'https port number')
    ]

    def initialize_options(self):
        self.riak_conf = None
        self.host = "127.0.0.1"
        self.pb_port = "8087"
        self.http_port = "8098"
        self.https_port = "18098"

    def finalize_options(self):
        if self.riak_conf is None:
            raise DistutilsOptionError("riak-conf option not set")

    def run(self):
        self.cert_dir = os.path.dirname(os.path.realpath(__file__)) + \
            "/riak/tests/resources"
        self._update_riak_conf()

    def _update_riak_conf(self):
        http_host = self.host + ':' + self.http_port
        https_host = self.host + ':' + self.https_port
        pb_host = self.host + ':' + self.pb_port
        self._backup_file(self.riak_conf)
        f = open(self.riak_conf, 'r', False)
        conf = f.read()
        f.close()
        conf = re.sub(r'search\s+=\s+off', r'search = on', conf)
        conf = re.sub(r'storage_backend\s+=\s+\S+',
                      r'storage_backend = leveldb',
                      conf)
        conf = re.sub(r'#*[ ]*listener.http.internal\s+=\s+\S+',
                      r'listener.http.internal = ' + http_host,
                      conf)
        conf = re.sub(r'listener.protobuf.internal\s+=\s+\S+',
                      r'listener.protobuf.internal = ' + pb_host,
                      conf)
        f = open(self.riak_conf, 'w', False)
        f.write(conf)
        f.close()

    def _backup_file(self, name):
        backup = name + ".bak"
        if os.path.isfile(name):
            shutil.copyfile(name, backup)
        else:
            log.info("Cannot backup missing file {0}".format(repr(name)))


class configure(Command):
    """
    Sets up security configuration.

    * Run create_bucket_types
    """

    description = "create bucket types for testing"

    user_options = create_bucket_types.user_options

    def initialize_options(self):
        self.riak_admin = None
        self.username = None
        self.password = None

    def finalize_options(self):
        bucket = self.distribution.get_command_obj('create_bucket_types')
        bucket.riak_admin = self.riak_admin

    def run(self):
        # Run all relevant sub-commands.
        for cmd_name in self.get_sub_commands():
            self.run_command(cmd_name)

    sub_commands = [('create_bucket_types', None)]
