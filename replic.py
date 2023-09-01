#!/usr/bin/env python3
# vim: ai ts=4 sts=4 et sw=4
import os, sys

if __name__ == '__main__':
    binary = '/usr/bin/poetry'
    args = [binary, 'run', 'replic'] + sys.argv[1:]
    os.execvp(binary, args)


