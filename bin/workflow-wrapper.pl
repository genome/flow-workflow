#!/usr/bin/env genome-perl

use Data::Dumper;
use File::Slurp qw/read_file/;
use Flow;
use IO::File;
use JSON;
use POSIX;
use above 'Genome';

use strict;
use warnings;


my $json = JSON->new->allow_nonref;
$| = 1; # forces perl to not buffer output to pipes


sub validate_arguments {
    my $expected_arguments = shift;

    if ($#_ != @$expected_arguments - 2) {
        printf(STDERR "Usage: $0 %s\n", join(' ', @$expected_arguments));
        exit 1;
    }
}


sub validate_method {
    my $method = shift;

    if ($method ne "shortcut" && $method ne "execute") {
        die "Invalid method '$method' -- must be 'shortcut' or 'execute'";
    }
}


sub load_inputs {
    my $file = shift;

    my $inputs_str = read_file($file);
    return {} if $inputs_str eq "";

    my $inputs = $json->decode($inputs_str);
    $inputs = Flow::decode_io_hash($inputs);
    return $inputs;
}


sub output_names {
    my $pkg = shift;

    return $pkg->__meta__->properties(is_output => 1);
}


sub get_command_outputs {
    my ($cmd, $pkg) = @_;

    my %outputs;
    for my $prop (output_names($pkg)) {
        my $prop_name = $prop->property_name;
        my $value = $prop->is_many ? [$cmd->$prop_name] : $cmd->$prop_name;
        $outputs{$prop_name} = $value;
    }

    $outputs{result} = 1 unless exists $outputs{result};

    return \%outputs;
}


sub run_event {
    validate_arguments(["event", "<shortcut|execute>", "<event_id>",
        "<outputs.json>"], @_);

    my ($method, $event_id, $outputs_file) = @_;

    validate_method($method);
    print STDERR "=========\n";
    print STDERR "Attempting to $method event $event_id...\n";
    print STDERR "vvvvvvvvv\n";

    my $event = Genome::Model::Event->get($event_id)
            || die "No event $event_id";

    if (!$event->can($method)) {
        print "Method '$method' not supported by event $event_id\n";
        exit(1);
    }
    my $ret = $event->$method();

    unless ($ret) {
        print STDERR "^^^^^^^^^\n";
        print STDERR "Failed with $method for event $event_id...\n";
        print STDERR "=========\n";

        exit(1);
    }
    UR::Context->commit();

    print STDERR "^^^^^^^^^\n";
    print STDERR "Succeeded with $method for event $event_id...\n";
    print STDERR "=========\n";

    Flow::write_outputs($outputs_file, { result => 1 });
}


sub run_command {
    validate_arguments(["command", "<shortcut|execute>", "<package>",
        "<inputs.json>", "<outputs.json>"], @_);

    my ($method, $pkg, $inputs_file, $outputs_file) = @_;

    validate_method($method);
    print STDERR "=========\n";
    print STDERR "Attempting to $method command $pkg...\n";
    print STDERR "vvvvvvvvv\n";

    eval "use $pkg";

    my $inputs = load_inputs($inputs_file);

    my $cmd = $pkg->create(%$inputs);
    if (!$pkg->can($method)) {
        print "$pkg does not support method '$method'\n";
        exit(1);
    }

    my $ret = $cmd->$method();

    unless ($ret) {
        print STDERR "^^^^^^^^^\n";
        print STDERR "Failed to $method command $pkg...\n";
        print STDERR "=========\n";

        exit(1);
    }

    UR::Context->commit();

    print STDERR "^^^^^^^^^\n";
    print STDERR "Succeeded to $method command $pkg...\n";
    print STDERR "=========\n";

    my $outputs = get_command_outputs($cmd, $pkg);
    Flow::write_outputs($outputs_file, $outputs);
}


# --- Main ---
if (@ARGV == 0) {
    print STDERR "Usage: $0 <action> <args>\n";
    exit(1);
}

my $action = shift @ARGV;
SWITCH: for ($action) {
    $_ eq "command" && do { run_command(@ARGV); last SWITCH; };
    $_ eq "event" && do { run_event(@ARGV); last SWITCH; };
    die "Unknown argument $_";
    exit 1;
}
