import ctypes
import os
import sys

import ds_basic

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
        self.cwd = self.session.open(('FileSystem', os.getcwd()), '<current object>')
        # switch to some other directory, so we don't prevent this one's deletion
        if os.path.sep == '/':
            os.chdir('/')
        elif os.path.sep == '\\':
            # We can't rely on a C:\\windows or even any envvars, so get the windows directory with an API call
            buf = ctypes.create_unicode_buffer(260)
            ctypes.windll.kernel32.GetWindowsDirectoryW(buf, 260)
            os.chdir(buf.value)

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
            if not args[i] or (args[i].count('"') % 2 == 1):
                args[i] = args[i] + args.pop(i+1)
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
                    try:
                        func = getattr(self, 'cmd_' + args[0])
                    except AttributeError:
                        self.prnt('I don\'t understand "%s". Type "help" if you need help.' % args[0])
                    else:
                        func(args[1:])
            except KeyboardInterrupt:
                self.prnt('Type "quit -f" if you really want to quit now')

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

def main(argv):
    s = Shell()
    return s.run()

if __name__ == '__main__':
    sys.exit(main(sys.argv))

