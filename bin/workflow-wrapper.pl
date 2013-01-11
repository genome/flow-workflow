#!/usr/bin/env genome-perl

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
    my $inputs = $json->decode($inputs_str);
    %$inputs = map {
        my $val = $inputs->{$_};
        $_ => $val eq '' ? '' : Flow::decode($val)
    } keys %$inputs;
    return $inputs;
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

    my $cmd = $pkg->create(%$inputs);
    exit(1) if !$pkg->can($method);
    my $ret = $cmd->$method();
    exit(1) unless $ret;

    %outputs = map {
        my $prop_name = $_->property_name;
        my $prop_val = $cmd->$prop_name;
        $prop_name => Flow::encode($prop_val);
    } @cmd_outputs;

    my $out_fh = new IO::File($outputs_file, "w");
    $out_fh->write($json->encode(\%outputs));
}

sub converge {
    if ($#_ < 2) {
        print STDERR "Usage: $0 converge <output-name> <inputs.json> <outputs.json> <input properties...>\n";
        exit 1;
    }

    my ($output_name, $inputs_file, $outputs_file, @input_properties) = @_;
    my $inputs = load_inputs($inputs_file);
    my $output_value = Flow::encode([
        map { ref($inputs->{$_}) eq 'ARRAY' ? @{$inputs->{$_}} : $inputs->{$_} } @input_properties
    ]);
    my %outputs = ($output_name => $output_value);
    my $out_fh = new IO::File($outputs_file, "w");
    $out_fh->write($json->encode(\%outputs));
}

if (@ARGV == 0) {
    print STDERR "Usage: $0 <action> <args>\n";
    exit(1);
}

my $action = shift @ARGV;
SWITCH: for ($action) {
    $_ eq "command" && do { run_command(@ARGV); last SWITCH; };
    $_ eq "converge" && do { converge(@ARGV); last SWITCH; };
    die "Unknown argument $_";
    exit 1;
}
