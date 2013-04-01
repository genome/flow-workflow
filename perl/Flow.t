#!/usr/bin/env perl

use strict;
use warnings;

use above 'Flow';
use Test::More;

my $inputs = {
    a => [1, 2, 3],
    b => {x => "y"},
    empty_string => "",
    msg => "hi",
    undefined => undef,
    zero => 0,
};

my $encoded = Flow::encode_io_hash($inputs);
my $decoded = Flow::decode_io_hash($encoded);

is_deeply([sort keys %$encoded], ["a", "b", "empty_string", "msg", "undefined", "zero"],
        "encoding doesn't encode io names");

is(ref($encoded->{a}), "ARRAY", "array values encoded as arrays");
is(ref($encoded->{b}), "HASH", "hash values encoded as hashes");
is($encoded->{undefined}, undef, "undef values encoded as undef");
is($encoded->{empty_string}, "", "empty string encoded as empty string");
is_deeply($inputs, $decoded, 'decode(x) = encode^{-1}(x)');

done_testing()
