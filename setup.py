from distutils.core import setup

setup(name='yastatsd',
    version='0.1.0',
    description='Yet Another Statsd Server',
    author='Gregory Haynes',
    author_email='greg@greghaynes.net',
    url='http://github.com/greghaynes/YAStatsd',
    license='MIT',
    packages=['yastatsd'],
    scripts=['scripts/yastatsd']
    )
