#!/usr/bin/env python3
# vim: ai ts=4 sts=4 et sw=4
import os, os.path, sys

if __name__ == '__main__':
    binary = '/usr/bin/poetry'
    args = [binary, 'run', 'replic'] + sys.argv[1:]
    os.chdir(os.path.abspath(os.path.dirname(sys.argv[0])))
    os.execvp(binary, args)


