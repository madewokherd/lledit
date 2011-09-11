import ctypes
import itertools
import os
import sys
import traceback

import ds_basic
import lledit_threads

class ShellJob(lledit_threads.Job):
    def __init__(self, description, shell, f, args=(), kwargs={}):
        kwargs = kwargs.copy()
        kwargs['progresscb'] = self.on_progress
        lledit_threads.Job.__init__(self, f , args, kwargs, self.on_finished)
        self.background = False
        self.canceled = False
        self.show_progress = False
        self.description = description
        self.shell = shell

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
    def __init__(self, shell, dsid):
        self.datastore = shell.session.open(dsid, '<temporary>')
        try:
            self.string_dsid = ds_basic.dsid_to_bytes(self.datastore.dsid)
            description = '<ls %s>' % self.string_dsid
            ShellJob.__init__(self, description, shell, self.run)
            self.results = []
            self.datastore.addref(self.description)
        finally:
            self.datastore.release('<temporary>')

    def on_finished(self, job):
        ShellJob.on_finished(self, job)
        if self.datastore:
            self.datastore.release(self.description)
        if not self.canceled:
            if self.exception:
                print 'ls in %s failed: %s' % (self.string_dsid, self.exception)
            else:
                self.shell.prnt("%i objects in %s:" % (len(self.results), self.string_dsid))
                for key in self.results:
                    self.shell.prnt(ds_basic.key_to_bytes(key))

    def run(self, progresscb):
        self.results = []
        for key in self.datastore.enum_keys(progresscb=progresscb):
            if self.canceled:
                break
            self.results.append(key)

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

    def prnt(self, string):
        print string

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

                if cmd[0] != 'quit':
                    self.threadpool.refresh()

                if args:
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
        return ds_basic.bytes_to_dsid(b, self.cwd.dsid)

    def cmd_ls(self, argv):
        """usage: ls [path]

List the objects contained by the current object, or a given path."""
        if len(argv) == 0:
            dsid = self.cwd.dsid
        else:
            dsid = self.bytes_to_dsid(argv[0])

        job = ShellListJob(self, dsid)

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

    def cmd_dir(self, argv):
        """usage: dir [path]

List the objects contained by the current object, or a given path."""
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

def main(argv):
    s = Shell()
    return s.run()

if __name__ == '__main__':
    sys.exit(main(sys.argv))

