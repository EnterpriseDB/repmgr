#!/usr/bin/env perl

use strict;
use warnings;


if (scalar @ARGV < 1) {
    print qq|Please provide path to the repmgr.conf file as first parameter\n|;
    exit(1);
}

my $file = $ARGV[0];

if (!-e $file) {
    print qq|Provide file "$file" does not exist\n|;
    exit(1);
}

if (!-f $file) {
    print qq|Provide path "$file" does not point to a file\n|;
    exit(1);
}

my @outp = ();
my $fh = undef;

if (!open ($fh, $file)){
    print qq|Unable to open "$file"\n|;
    exit(1);
}

my $data_directory_found = 0;

while(<$fh>) {
    my $line = $_;
    chomp $line;
    if ($line =~ m|\s*#|) {
        push @outp, $line;
        next;
    }

    if ($line !~ m|\s*(\S+?)\s*=(.+?)$|) {
        push @outp, $line;
        next;
    }

    my $param = $1;
    my $value = $2;

    # These parameters can be removed:
    next if ($param eq 'cluster');

    # These parameters can be renamed:
    if ($param eq 'node') {
        push @outp, qq|node_id=${value}|;
    }
    elsif ($param eq 'loglevel') {
        push @outp, qq|log_level=${value}|;
    }
    elsif ($param eq 'logfacility') {
        push @outp, qq|log_facility=${value}|;
    }
    elsif ($param eq 'logfile') {
        push @outp, qq|log_file=${value}|;
    }
    elsif ($param eq 'master_response_timeout') {
        push @outp, qq|async_query_timeout=${value}|;
    }
    elsif ($param eq 'retry_promote_interval_secs') {
        push @outp, qq|primary_notification_timeout=${value}|;
    }
    else {
        if ($param eq 'data_directory') {
            $data_directory_found = 1;
        }

        # Don't quote numbers
        if ($value =~ /^\d+$/) {
            push @outp, sprintf(q|%s=%s|, $param, $value);
        }
        # Quote everything else
        else {
            $value =~ s/'/''/g;
            push @outp, sprintf(q|%s='%s'|, $param, $value);
        }
    }
}

close $fh;

print join("\n", @outp);
print "\n";

if ($data_directory_found == 0) {
    print "data_directory=''\n";
}

