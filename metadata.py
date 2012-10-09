import subprocess
import os.path

try:
    # don't get confused if our sdist is unzipped in a subdir of some 
    # other hg repo
    if os.path.isdir('.hg'):
        p = subprocess.Popen(['hg', 'parents', r'--template={rev}\n'],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if not p.returncode:
            fh = open('VCSREV', 'w')
            fh.write(p.communicate()[0].splitlines()[0])
            fh.close()

    elif os.path.isdir('.git'):
        p = subprocess.Popen(['git', 'rev-parse', r'HEAD'],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if not p.returncode:
            fh = open('VCSREV', 'w')
            fh.write(p.communicate()[0].splitlines()[0])
            fh.close()

except (OSError, IndexError):
    pass
    
try:
    rev = open('VCSREV').read()
except IOError:
    rev = ''

name = os.path.basename(os.path.dirname(os.path.abspath(__file__)))
authors = 'Ivan Zakrevsky'
copyright_years = '2011-2012'
version = '0.7.post{0}'.format(rev)
release = version
