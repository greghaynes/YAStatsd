Yet Another Statsd
==================

A wireline compatible statsd server written in python.

## Setup

The recommended method of usage is inside a virtualenv.

### Install dependencies
<pre>
pip install -r requirements.txt
</pre>

### Configure
Edit config.py to your liking

### Run
<pre>
python main.py
</pre>


## Using

YAStatsd aims to be wireline compatible with the original nodejs statsd
implementation. Information on how to send data to statsd can be found here:

https://github.com/etsy/statsd/blob/master/README.md

