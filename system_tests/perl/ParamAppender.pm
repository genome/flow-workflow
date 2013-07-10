package ParamAppender;

use UR;

use strict;
use warnings;

class ParamAppender {
    is => "Command::V2",
    has_input => [
        prefix => {
            is => "Text",
        },
        suffix => {
            is => "Text",
        },
    ],
    has_output => [
        output => {
            is => "Text",
        },
    ],
};

sub execute {
    my $self = shift;
    my $p = $self->prefix;
    my $s = $self->suffix;
    my $a = "$p$s";
    print "$p + $s = $a!!\n";
    $self->output($a);
    return 1;
};

1;
