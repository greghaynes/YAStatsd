from setuptools import setup

setup(name='yastatsd',
    version='0.1.1',
    description='Yet Another Statsd Server',
    author='Gregory Haynes',
    author_email='greg@greghaynes.net',
    url='http://github.com/greghaynes/YAStatsd',
    license='MIT',
    install_requires=['Twisted>=11.0.0'],
    packages=['yastatsd', 'yastatd.backends'],
    scripts=['scripts/yastatsd']
    )
