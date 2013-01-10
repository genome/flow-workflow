package Flow;

use Storable qw/nfreeze thaw/;
use Carp qw/confess/;
use POSIX qw/WIFEXITED WEXITSTATUS/;
use JSON;
use Workflow;
use File::Temp qw/tempdir/;
use Data::Dumper;
use MIME::Base64;
use strict;
use warnings;

sub _encode_scalar {
    my $obj = shift;
    return MIME::Base64::encode(nfreeze(\$obj));
}

sub _decode_scalar {
    my $str = shift;
    return ${thaw(MIME::Base64::decode($str))};
}

sub _encode_array {
    my $arrayref = shift;
    my @encoded = map {_encode_scalar($_)} @$arrayref;
    return \@encoded;
}

sub _decode_array {
    my $arrayref = shift;
    my @decoded = map {_decode_scalar($_)} @$arrayref;
    return \@decoded;
}

sub encode {
    my $obj = shift;
    if (ref($obj) eq "ARRAY") {
        $obj = _encode_array($obj);
    } else {
        $obj = _encode_scalar($obj);
    }
    my $json = new JSON->allow_nonref;
    return $json->encode($obj);
}

sub decode {
    my $str = shift;
    my $json = new JSON->allow_nonref;
    my $obj = $json->decode($str);
    if (ref($obj) eq "ARRAY") {
        return _decode_array($obj);
    } else {
        return _decode_scalar($obj);
    }
}

sub run_workflow {
    my $workflow = shift;
    my $xml = $workflow;

    my $tmpdir = tempdir(CLEANUP => 1);
    if (ref($workflow)) {
        $xml = join("/", $tmpdir, "workflow.xml");
        my $xml_fh = new IO::File($xml, "w");
        $xml_fh->write($workflow->save_to_xml());
        $xml_fh->close();
    }

    my %params = @_;
    %params = map {$_ => encode($params{$_})} keys %params;
    print Dumper(\%params);

    my $json_path = join("/", $tmpdir, "inputs.json");
    my $json_fh = new IO::File($json_path, "w");

    print "Saved xml to $xml\n";
    my $json = new JSON->allow_nonref;
    $json_fh->write($json->encode(\%params));
    $json_fh->close();
    my $cmd = "submit-workflow $xml $json_path --block";
    print "EXEC: $cmd\n";
    my $ret = system($cmd);
    if (!WIFEXITED($ret) || WEXITSTATUS($ret)) {
        confess "Workflow submission failed";
    }
    return 1;
}

1;
