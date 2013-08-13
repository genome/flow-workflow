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


sub now {
    return POSIX::strftime('%Y-%m-%d %H:%M:%S', localtime(time()));
}

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

sub complete_event {
    my $event = shift;
    my $status = shift;

    set_event_params($event, {
        date_completed => now(),
        event_status => $status,
    });
}


sub set_event_params {
    my $event = shift;
    my $params = shift;

    UR::Context->rollback();

    for my $param_name (keys %$params) {
        my $param_value = $params->{$param_name};
        $event->$param_name($param_value);
    }

    UR::Context->commit();
}

sub print_exit_message {
    my $message = shift;

    print STDERR "^^^^^^^^^\n";
    print STDERR $message;
    print STDERR "=========\n";
}

sub exit_wrapper {
    my $message = shift;

    print_exit_message($message);
    exit(1);
}

sub run_event {
    validate_arguments(["event", "<shortcut|execute>", "<event_id>",
        "<outputs.json>"], @_);

    my ($method, $event_id, $outputs_file) = @_;

    print STDERR "=========\n";
    print STDERR "Attempting to $method event $event_id...\n";
    print STDERR "vvvvvvvvv\n";

    validate_method($method);

    my $event = Genome::Model::Event->get($event_id);
    unless ($event) {
        exit_wrapper("Could not find event for event_id $event_id\n");
    }

    set_event_params($event, {
        date_scheduled => now(),
        event_status => 'Running',
        lsf_job_id => $ENV{'LSB_JOBID'},
        user_name => $ENV{'USER'},
    });

    if (!$event->can($method)) {
        print STDERR "Method '$method' not supported by event $event_id\n";
        exit_wrapper("Failed with $method for event $event_id...\n");
    }

    my $ret = eval { $event->$method() };
    my $error = $@;

    if ($error) {
        complete_event($event, 'Crashed');
        exit_wrapper("Crashed with $method for event $event_id...\n");
    }

    unless ($ret) {
        complete_event($event, 'Failed');
        exit_wrapper("Failed with $method for event $event_id...\n");
    }
    UR::Context->commit();

    complete_event($event, 'Succeeded');
    print_exit_message("Succeeded with $method for event $event_id...\n");

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
        exit_wrapper("$pkg does not support method '$method'\n");
    }

    my $ret = eval { $cmd->$method() };
    my $error = $@;

    if ($error) {
        exit_wrapper("Crashed in $method for command $pkg...\n");
    }

    unless ($ret) {
        exit_wrapper("Failed to $method command $pkg...\n");
    }

    UR::Context->commit();

    print_exit_message("Succeeded to $method command $pkg...\n");

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
