package Flow;

use Carp qw/confess/;
use Data::Dumper;
use File::Slurp qw/read_file/;
use File::Temp qw/tempdir/;
use JSON;
use MIME::Base64;
use POSIX qw/WIFEXITED WEXITSTATUS/;
use Storable qw/nfreeze thaw/;
use Workflow;

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

sub _encode_hash {
    my $hashref = shift;
    my %encoded = map {_encode_scalar($_) => _encode_scalar($hashref->{$_})} keys %$hashref;
    return \%encoded;
}

sub _decode_hash {
    my $hashref = shift;
    my %decoded = map {_decode_scalar($_) => _decode_scalar($hashref->{$_})} keys %$hashref;
    return \%decoded;
}

sub encode {
    my $obj = shift;
    if (ref($obj) eq "ARRAY") {
        $obj = _encode_array($obj);
    } elsif (ref($obj) eq "HASH") {
        $obj = _encode_hash($obj);
    } else {
        $obj = _encode_scalar($obj);
    }
    return $obj;
}

sub decode {
    my $obj = shift;
    if (ref($obj) eq "ARRAY") {
        return _decode_array($obj);
    } elsif (ref($obj) eq "HASH") {
        $obj = _decode_hash($obj);
    } else {
        return _decode_scalar($obj);
    }
}

sub encode_io_hash {
    my $io = shift;
    return {
        map {
            my $val = $io->{$_};
            $_ => $val eq '' ? '' : Flow::encode($val)
        } keys %$io
    };

}

sub decode_io_hash {
    my $io = shift;
    return {
        map {
            my $val = $io->{$_};
            $_ => $val eq '' ? '' : Flow::decode($val);
        } keys %$io
    };
}

sub run_workflow {
    my $wf_repr = shift;
    my $result;

    my $r = ref($wf_repr);
    if ($r) {
        if ($r eq 'GLOB') {
            $result = _run_from_file($wf_repr, @_);
        } elsif (UNIVERSAL::isa($wf_repr, 'Workflow::Operation')) {
            $result = _run_from_object($wf_repr, @_);
        } else {
            die 'unrecognized reference';
        }
    } elsif (-f $wf_repr) {
        $result = _run_from_file($wf_repr, @_);
    } else {
        $result = _run_from_string($wf_repr, @_);
    }

    return $result;
}

sub _run_from_object {
    my $wf_operation = shift;

    print "Running workflow from Workflow::Operation object\n";

    my $wf_string = $wf_operation->save_to_xml();
    return _run_from_string($wf_string, @_);
}

sub _run_from_file {
    my $file_handle_or_name = shift;

    print "Running workflow from file\n";

    my $wf_string = read_file($file_handle_or_name);
    return _run_from_string($wf_string, @_);
}

sub _run_from_string {
    my $wf_string = shift;

    my %params = @_;

    print "Running workflow from string\n";

    my $cleanup = !exists $ENV{FLOW_WORKFLOW_NO_CLEANUP};
    my $tmpdir = tempdir(CLEANUP => $cleanup);

    my $xml_path = _write_xml($tmpdir, $wf_string);
    my $inputs_path = _write_inputs($tmpdir, \%params);
    my $outputs_path = join("/", $tmpdir, "outputs.json");

    my $cmd = "flow submit-workflow --xml $xml_path --inputs-file $inputs_path " .
        "--block --outputs-file $outputs_path";
    print "EXEC: $cmd\n";

    my $ret = system($cmd);
    if (!WIFEXITED($ret) || WEXITSTATUS($ret)) {
        confess "Workflow submission failed";
    }

    if (-s $outputs_path) {
        print "run_workflow got some outputs:\n";
        return _read_outputs($outputs_path);
    }
    else {
        print "run_workflow got no outputs :(\n";
        return 1;
    }
}

sub _write_xml {
    my ($tmpdir, $wf_string) = @_;

    my $xml_path = join("/", $tmpdir, "workflow.xml");
    my $xml_fh = new IO::File($xml_path, "w");
    $xml_fh->write($wf_string);
    $xml_fh->close();
    print "Saved xml to $xml_path\n";

    return $xml_path;
}

sub _write_inputs {
    my ($tmpdir, $params) = @_;

    print "WORKFLOW PARAMS:" . Dumper($params);

    my $inputs_path = join("/", $tmpdir, "inputs.json");
    my $encoded_params = encode_io_hash($params);
    my $json_fh = new IO::File($inputs_path, "w");

    my $json = new JSON->allow_nonref;
    $json_fh->write($json->encode($encoded_params));
    $json_fh->close();

    return $inputs_path;
}

sub _read_outputs {
    my $outputs_path = shift;

    my $outputs_str = read_file($outputs_path);
    my $json = new JSON->allow_nonref;
    my $outputs = $json->decode($outputs_str);

    return decode_io_hash($outputs);
}

1;
