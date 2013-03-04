#!/usr/bin/env perl

use strict;
use warnings;

use above 'Flow';
use Test::More;

my $inputs = {
    a => [1, 2, 3],
    b => {x => "y"},
    msg => "hi",
    empty => "",
};

my $encoded = Flow::encode_io_hash($inputs);
my $decoded = Flow::decode_io_hash($encoded);

is_deeply([sort keys %$encoded], ["a", "b", "empty", "msg"],
        "encoding doesn't encode io names");

is(ref($encoded->{a}), "ARRAY", "array values encoded as arrays");
is(ref($encoded->{b}), "HASH", "hash values encoded as hashes");
is($encoded->{c}, undef, "empty values encoded as empty");
is_deeply($inputs, $decoded, 'decode(x) = encode^{-1}(x)');

done_testing()
