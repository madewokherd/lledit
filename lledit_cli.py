import ctypes
import itertools
import optparse
import os
import readline
import sys
import traceback

import ds_basic
import lledit_threads

class ShellJob(lledit_threads.Job):
    def __init__(self, description, shell):
        lledit_threads.Job.__init__(self, self.run, (), {}, self.on_finished)
        self.background = False
        self.canceled = False
        self.show_progress = False
        self.description = description
        self.shell = shell

    def run(self):
        pass

    def on_finished(self, job):
        if self.canceled:
            self.shell.prnt("Job %s (%s) canceled." % (self.id, self.description))
        elif self.background:
            self.shell.prnt("Job %s (%s) finished." % (self.id, self.description))

    def on_progress(self, part, whole, *args):
        if self.canceled:
            raise KeyboardInterrupt()
        elif self.show_progress and not self.background:
            pass # FIXME

class ShellListJob(ShellJob):
    def __init__(self, shell, dsid, longformat):
        self.longformat = longformat
        self.maxlen = 1
        self.datastore = shell.session.open(dsid, '<temporary>')
        try:
            self.string_dsid = ds_basic.dsid_to_bytes(self.datastore.dsid)
            description = '<ls %s>' % self.string_dsid
            ShellJob.__init__(self, description, shell)
            self.results = []
            self.datastore.addref(self.description)
        finally:
            self.datastore.release('<temporary>')

    def on_finished(self, job):
        ShellJob.on_finished(self, job)
        self.datastore.release(self.description)
        if not self.canceled:
            if self.exception:
                print 'ls in %s failed:\n%s' % (self.string_dsid, self.traceback)
            else:
                self.shell.prnt("%i objects in %s:" % (len(self.results), self.string_dsid))
                for i, key in enumerate(self.results):
                    name = ds_basic.key_to_bytes(key)
                    if self.longformat:
                        spacing = ' ' * (self.maxlen + 3 - len(name))
                        description = self.descriptions[i]
                        self.shell.prnt('%s%s%s' % (name, spacing, description))
                    else:
                        self.shell.prnt(name)

    def run(self):
        self.results = []
        self.descriptions = []
        for key in self.datastore.enum_keys(progresscb=self.on_progress):
            if self.canceled:
                break
            self.results.append(key)
            if self.longformat:
                if isinstance(key, ds_basic.BrokenData):
                    self.descriptions.append('')
                else:
                    self.maxlen = max(self.maxlen, len(ds_basic.key_to_bytes(key)))
                    item_datastore = self.datastore.open((key,), '<temporary>')
                    try:
                        description = item_datastore.get_description()
                    except:
                        description = 'Failure reading object'
                    finally:
                        item_datastore.release('<temporary>')
                    self.descriptions.append(description)

class ShellReadJob(ShellJob):
    def __init__(self, shell, dsid, hex_format, newline):
        self.hex_format = hex_format
        self.newline = newline
        self.datastore = shell.session.open(dsid, '<temporary>')
        try:
            self.string_dsid = ds_basic.dsid_to_bytes(self.datastore.dsid)
            description = '<read %s>' % self.string_dsid
            ShellJob.__init__(self, description, shell)
            self.results = []
            self.datastore.addref(self.description)
        finally:
            self.datastore.release('<temporary>')

    def on_progress(self, part, whole, data):
        if data:
            self.results.append(data)
        ShellJob.on_progress(self, part, whole)

    def on_finished(self, job):
        ShellJob.on_finished(self, job)
        self.datastore.release(self.description)
        if not self.canceled:
            if self.exception:
                print 'reading %s failed:\n%s' % (self.string_dsid, self.traceback)
            else:
                if self.hex_format:
                    bytes_per_line = (self.shell.width + 1 / 3)
                    bytes = []
                    for res in self.results:
                        bytes.extend('%02X' % ord(c) for c in res)
                        while len(bytes) >= bytes_per_line:
                            self.shell.prnt(' '.join(bytes[0:bytes_per_line]))
                            bytes = bytes[bytes_per_line:]
                    if bytes:
                        self.shell.prnt(' '.join(bytes))
                else:
                    for res in self.results:
                        self.shell.prnt(res, newline=False)
                    if self.newline:
                        self.shell.prnt('')

    def run(self):
        self.results = []
        self.datastore.read_bytes(ds_basic.ALL, progresscb=self.on_progress)

class ShellWriteJob(ShellJob):
    def __init__(self, shell, dest_path, src_path):
        self.modified = ()
        self.dest_datastore = shell.session.open(dest_path, '<temporary>')
        try:
            self.src_datastore = shell.session.open(src_path, '<temporary>')
            try:
                self.dest_path = ds_basic.dsid_to_bytes(self.dest_datastore.dsid)
                self.src_path = ds_basic.dsid_to_bytes(self.src_datastore.dsid)
                description = '<write %s to %s>' % (self.src_path, self.dest_path)
                ShellJob.__init__(self, description, shell)
                self.src_datastore.addref(self.description)
            finally:
                self.src_datastore.release('<temporary>')
            self.dest_datastore.addref(self.description)
        finally:
            self.dest_datastore.release('<temporary>')

    def on_finished(self, job):
        ShellJob.on_finished(self, job)
        self.dest_datastore.release(self.description)
        self.src_datastore.release(self.description)
        if not self.canceled and self.exception:
            print 'writing %s to %s failed:\n%s' % (self.src_path, self.dest_path, self.traceback)
        elif not self.canceled:
            if self.modified:
                for ds in self.modified:
                    print 'Unsaved changes to %s' % (ds_basic.dsid_to_bytes(ds.dsid))

    def run(self):
        self.modified = self.dest_datastore.write(self.src_datastore, self, {}, self.on_progress)

class Shell(object):
    easteregg_strings = {
        'love': "I have not been taught how to love.",
        'me': """I can only help with a limited set of things.
To see the list of things I can help with, type "help topics".

If you're unsure where to turn, try http://www.allaboutcounseling.com/""",
    }

    help_strings = {
        'default':
"""The most broadly useful commands are:
   quit        Quit lledit
   ls          Find out where you can go
   cd          Change where you are
   read        View the data in an object (usually a file)
   write       Modify the data in an object
   save        Save your changes
   open        Create a name for an object
   close       Remove your name for an object

To see a list of all commands and help topics, type "help topics\""""
        }

    width = 80 # FIXME

    quits = 0

    def __init__(self):
        self.session = ds_basic.Session()
        self.threadpool = lledit_threads.ThreadPool()
        self.cwd = self.session.open(('FileSystem', os.getcwd()), '<current object>')
        # switch to some other directory, so we don't prevent this one's deletion
        if os.path.sep == '/':
            os.chdir('/')
        elif os.path.sep == '\\':
            # We can't rely on a C:\\windows or even any envvars, so get the windows directory with an API call
            buf = ctypes.create_unicode_buffer(260)
            ctypes.windll.kernel32.GetWindowsDirectoryW(buf, 260)
            os.chdir(buf.value)
        self.jobs = {}

    def prnt(self, string, newline=True):
        if newline:
            print string
        else:
            print string,

    def readline(self, prompt):
        try:
            return raw_input(prompt)
        except EOFError:
            self.prnt('')
            return 'quit -f'

    def split(self, string):
        args = string.split(' ')
        i = 0
        while i < len(args) - 1:
            if (args[i].count('"') % 2 == 1):
                args[i] = args[i] + ' ' + args.pop(i+1)
            elif not args[i]:
                args.pop(i)
            else:
                i += 1
        if args and not args[i]:
            args.pop(i)
        return args

    def prompt(self):
        return '%s> ' % ds_basic.dsid_to_bytes(self.cwd.dsid)

    def run(self):
        self.prnt("lledit shell")
        self.prnt('Type "help" for more information')

        while self.quits <= 0:
            try:
                cmd = self.readline(self.prompt())
                args = self.split(cmd)

                if args:
                    if args[0] != 'quit':
                        self.threadpool.refresh()

                    if args[0] == 'pyeval':
                        # this isn't a "real" command, so I don't have to document or maintain it :p
                        self.prnt(eval(' '.join(args[1:])))
                    else:
                        try:
                            func = getattr(self, 'cmd_' + args[0])
                        except AttributeError:
                            self.prnt('I don\'t understand "%s". Type "help" if you need help.' % args[0])
                        else:
                            func(args[1:])

                self.threadpool.refresh()
            except KeyboardInterrupt:
                self.prnt('Type "quit -f" if you really want to quit now')
            except BaseException, e:
                traceback.print_exc()

        self.quits -= 1

    def cmd_quit(self, argv):
        """usage: quit [-f]

Quit the lledit shell. If -f is included, don't ask about unsaved files."""
        self.quits += 1

    def cmd_help(self, argv):
        """usage: help [topic]

Prints hopefully helpful information. You're using it right now."""
        if argv:
            topic = argv[0]
        else:
            topic = 'default'
        if topic == 'topics':
            topics = set(['topics'])
            for attr in dir(self):
                if attr.startswith('cmd_'):
                    topics.add(attr[4:])
            for klass in type(self).mro():
                try:
                    topics.update(klass.help_strings)
                except AttributeError:
                    pass
            self.prnt("I can provide help with the following topics:")
            for topic in sorted(topics):
                print '    ' + topic
            return
        try:
            func = getattr(self, 'cmd_'+topic)
        except AttributeError:
            pass
        else:
            self.prnt(func.func_doc)
            return
        for klass in type(self).mro():
            try:
                string = klass.help_strings[topic]
            except (AttributeError, KeyError):
                pass
            else:
                self.prnt(string)
                return

            try:
                string = klass.easteregg_strings[topic]
            except (AttributeError, KeyError):
                pass
            else:
                self.prnt(string)
                return
        self.prnt('I don\'t know anything about "%s"' % topic)

    def cmd_pwd(self, argv):
        """usage: pwd

Print the id of the object you're currently working with."""
        self.prnt(ds_basic.dsid_to_bytes(self.cwd.dsid))

    def bytes_to_dsid(self, b):
        return ds_basic.bytes_to_dsid(b, self.cwd.dsid, self.session)

    def do_job(self, job):
        self.threadpool.queue_job(job)

        try:
            self.threadpool.wait_for_job(job, 0.2)
            if not job.finished:
                job.show_progress = True
                self.threadpool.wait_for_job(job)
        except KeyboardInterrupt:
            if not job.finished:
                for i in itertools.count():
                    if i not in self.jobs:
                        break
                job.background = True
                self.jobs[i] = job
                self.prnt('Running job %i: %s in the background; use "cancel %i" to stop' % (i, job.description, i))

    def cmd_ls(self, argv):
        """usage: ls [path]

List the objects contained by the current object, or a given path.

If -l is specified, print more information about the objects listed (this may
involve reading them)."""
        longformat = False

        if argv and argv[0] == '-l':
            argv = argv[1:]
            longformat = True

        if len(argv) == 0:
            dsid = self.cwd.dsid
        else:
            dsid = self.bytes_to_dsid(argv[0])

        job = ShellListJob(self, dsid, longformat)

        self.do_job(job)

    def cmd_dir(self, argv):
        """usage: dir [-l] [path]

List the objects contained by the current object, or a given path.

If -l is specified, print more information about the objects listed (this may
involve reading them)."""
        return self.cmd_ls(argv)

    def cmd_cd(self, argv):
        """usage: cd path

Change the current working object to the given path."""

        if len(argv) == 0:
            self.prnt("cd: requires a path")
            return

        dsid = self.bytes_to_dsid(argv[0])

        new_cwd = self.session.open(dsid, '<current object>')

        try:
            self.cwd.release('<current object>')
            self.cwd = new_cwd
        except:
            new_cwd.release('<current object>')
            raise

    def cmd_read(self, argv):
        """usage: read [-hn] [path]

Read bytes from an object. If no path is specified, use the current working
object.

If the -h switch is specified, print the data in hex format.

If the -n switch is specified, do not print a newline after the data.

For most objects with data, you can specify a slice as the path, to read only
some data. For example, "read 3..." will read all the data in a file starting
from the fourth byte, and 10..12 will read two bytes of data starting from the
10th byte."""
        hex_format = False
        newline = True
        if argv and argv[0].startswith('-'):
            switches = set(argv.pop(0))
            switches.remove('-')
            if 'h' in switches:
                hex_format = True
                switches.remove('h')
            if 'n' in switches:
                newline = False
                switches.remove('n')
            if switches:
                self.prnt('read: unrecognized switches %s' % ''.join(switches))
                return

        if len(argv) == 0:
            dsid = self.cwd.dsid
        else:
            dsid = self.bytes_to_dsid(argv[0])

        job = ShellReadJob(self, dsid, hex_format, newline)

        self.do_job(job)

    def cmd_write(self, argv):
        """usage: write [-d dest_path] [-s src_path]

Write to an object.

If the -d switch is specified, write to the given path, otherwise use the
current object.

If the -s switch is specified, read from the given path."""
        parser = optparse.OptionParser()
        parser.add_option('-d', action='store', type='string', dest='dest_path')
        parser.add_option('-s', action='store', type='string', dest='src_path')
        options, args = parser.parse_args(argv)

        if args:
            self.prnt('write: should be called with no arguments, only switches')
            return

        if options.dest_path:
            dest_path = self.bytes_to_dsid(options.dest_path)
        else:
            dest_path = self.cwd.dsid

        if options.src_path:
            src_path = self.bytes_to_dsid(options.src_path)
        else:
            self.prnt('write: a source path must be specified; use the -s switch')
            return

        job = ShellWriteJob(self, dest_path, src_path)

        self.do_job(job)

    def cmd_lsof(self, argv):
        """usage: lsof

Get a list of paths that are opened and why."""
        parser = optparse.OptionParser()
        options, args = parser.parse_args(argv)

        if args:
            self.prnt('lsof: should be called with no arguments')
            return

        items = self.session.get_open_datastores()
        longest_path = 0
        for path, reasons in items:
            longest_path = max(len(ds_basic.dsid_to_bytes(path)), longest_path)
        longest_path += 3
        for path, reasons in items:
            byte_path = ds_basic.dsid_to_bytes(path)
            byte_reasons = []
            for reason in reasons:
                if isinstance(reason, basestring):
                    byte_reasons.append(reason)
                else:
                    byte_reasons.append(ds_basic.dsid_to_bytes(reason))
            print '%s%s%s' % (byte_path, ' ' * (longest_path - len(byte_path)), ' '.join(byte_reasons))

def main(argv):
    s = Shell()
    return s.run()

if __name__ == '__main__':
    sys.exit(main(sys.argv))

