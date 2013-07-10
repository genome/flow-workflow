#!/usr/bin/env perl

use Flow;
use JSON;

use strict;
use warnings;

my $outputs = Flow::read_outputs($ARGV[0]);
my $json = new JSON->allow_nonref;

printf("%s\n", $json->encode($outputs));
