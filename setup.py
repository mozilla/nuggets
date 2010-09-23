import fnmatch
import os
from distutils.core import setup

files = os.listdir(os.path.dirname(os.path.realpath(__file__)))
py_modules = [os.path.splitext(f)[0]
              for f in fnmatch.filter(files, '*.py') if f != 'setup.py']

setup(
    name='nuggets',
    version='0.1',
    py_modules=py_modules,
    description="Little utilities that don't deserve a package",
    long_description=open('README.rst').read(),
    author='Jeff Balogh',
    author_email='jbalogh@mozilla.com',
    url='http://github.com/mozilla/nuggets',
    license='BSD',
    zip_safe=False,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Framework :: Django',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
