#!/usr/bin/env genome-perl

use POSIX;
use above 'Genome';
use IO::File;
use File::Slurp qw/read_file/;
use Data::Dumper;
use JSON;
use Flow;
use strict;
use warnings;

my $json = JSON->new->allow_nonref;

sub load_inputs {
    my $file = shift;
    my $inputs_str = read_file($file);
    return {} if $inputs_str eq "";
    my $inputs = $json->decode($inputs_str);
    $inputs = Flow::decode_io_hash($inputs);
    return $inputs;
}

sub run_event {
    if ($#_ != 2) {
        print STDERR "Usage: $0 event <shortcut|execute> <event_id> <outputs.json>\n";
        exit 1;
    }

    my ($method, $event_id, $outputs_file) = @_;
    if ($method ne "shortcut" && $method ne "execute") {
        die "Invalid method '$method' for command: $method";
    }

    my $cmd = Genome::Model::Event->get($event_id) || die "No event $event_id";
    exit(1) if !$cmd->can($method);
    my $ret = $cmd->$method();
    exit(1) unless $ret;
    UR::Context->commit();

    my %outputs = (result => Flow::encode(1));
    my $out_fh = new IO::File($outputs_file, "w");
    $out_fh->write($json->encode(\%outputs));
}

sub run_command {
    if ($#_ != 3) {
        print STDERR "Usage: $0 command <shortcut|execute> <package> <inputs.json> <outputs.json>\n";
        exit 1;
    }

    my ($method, $pkg, $inputs_file, $outputs_file) = @_;
    if ($method ne "shortcut" && $method ne "execute") {
        die "Invalid method '$method' for command: $method";
    }

    my @cmd_inputs = $pkg->__meta__->properties(is_input => 1);
    my @cmd_outputs = $pkg->__meta__->properties(is_output => 1);
    my %outputs;
    my $inputs = load_inputs($inputs_file);

    print "Creating command $pkg with inputs " . Dumper($inputs);
    my $cmd = $pkg->create(%$inputs);
    exit(1) if !$pkg->can($method);
    my $ret = $cmd->$method();
    exit(1) unless $ret;


    %outputs = map {
        my $prop_name = $_->property_name;
        $prop_name => $cmd->$prop_name
    } @cmd_outputs;
    $outputs{result} = 1 unless exists $outputs{result};

    my $outputs = Flow::encode_io_hash(\%outputs);

    UR::Context->commit();

    my $out_fh = new IO::File($outputs_file, "w");
    $out_fh->write($json->encode($outputs));
}

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
