package NullCommand;

use File::Path qw(make_path);
use File::Basename qw(dirname);
use UR;

class NullCommand {
    is => "Command::V2",
    has_input => [
        res1 => {
            is => "Text",
            is_optional => 1,
        },
        res2 => {
            is => "Text",
            is_optional => 1,
        },
        catcher => {
            is => "Text",
            is_optional => 1,
        },
        param => {
            is => "Text",
            doc => "A number",
        },
    ],
};

sub execute {
    my $self = shift;
    print "param: " . $self->param . "\n";
    print "catcher: " . $self->catcher . "\n";
    return 1;
}

1;
