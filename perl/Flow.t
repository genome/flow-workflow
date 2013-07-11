#!/usr/bin/env perl

use Test::More;
use Data::Dumper;

use strict;
use warnings;

use above 'Flow';

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
is_deeply($inputs, $decoded, 'decode(x) = encode^{-1}(x)');

my $nested_inputs = {
    many_things => [
        [1, 2, 3],
        [4, 5],
        [6],
    ],
};

my $expected_encoded_inputs = {
    many_things => [
        [
            Flow::_encode_scalar(1),
            Flow::_encode_scalar(2),
            Flow::_encode_scalar(3),
        ],
        [
            Flow::_encode_scalar(4),
            Flow::_encode_scalar(5),
        ],
        [
            Flow::_encode_scalar(6),
        ],
    ],
};
my $encoded_inputs = Flow::encode_io_hash($nested_inputs);

is_deeply($expected_encoded_inputs, $encoded_inputs, 'nested encoding wins');

done_testing();
