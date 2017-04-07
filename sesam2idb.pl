#!/usr/bin/perl
################################################################################
#
# Name:  sesam2idb.pl
# Lang:  perl
# Date:  2016-03-29
# Author: emueller@ennit.de
#
# DESCRIPTION:
#	This script queries the SEP database for backup data and writes the 
#	results into IDB.
#
# REQUIREMENTS:
#	- JSON (libjson-perl)
#	- curl (curl)
#
#	This script must be run on the sesam server!
#
#
# last edited 2017-03-27 emueller@ennit.de
#
################################################################################

use warnings;
use strict;
use JSON;
use Data::Dumper;
use Getopt::Std;
use vars qw(%opt);

###################
## Config start

# IDB URL
my $idb_url = 'https://idb.domain.tld/api/v2/machines';
# IDB API Token
my $idb_api_token = '';

# Sesam sm_db binary (full path)
my $sm_db = '/opt/sesam/bin/sesam/sm_db';
# Sesam profile (full path)
my $sm_profile = '/var/opt/sesam/var/ini/sesam2000.profile';

# curl binary (full path)
my $curl = '/usr/bin/curl';

## Config end
###################


###################
## Main start

my $DEBUG = 0;

getopts('dh', \%opt) or &help();
$opt{'h'} and &help();
$opt{'d'} and $DEBUG = 1;

my $now = qx{date}; chomp($now);
my $cmd = 'curl -s -k -X GET '.$idb_url.'?idb_api_token='.$idb_api_token;
$DEBUG and print "DEBUG: cmd = $cmd\n";

my $res_json = qx{$cmd};
my $res = from_json($res_json);
foreach my $server (@{$res}) {
	print "Server: ".$server->{fqdn}."\n";
	my $skip = 0;

# prepare sesam database queries
	my $query_f = "select sesam_date,stop_time,data_size,task FROM results WHERE client like '".$server->{fqdn}."%' AND copy_from IS NULL AND fdi_type='F' ORDER BY sesam_date DESC limit 1";
	my $query_d = "select sesam_date,stop_time,data_size,task FROM results WHERE client like '".$server->{fqdn}."%' AND copy_from IS NULL AND fdi_type='D' ORDER BY sesam_date DESC limit 1";
	my $query_i = "select sesam_date,stop_time,data_size,task FROM results WHERE client like '".$server->{fqdn}."%' AND copy_from IS NULL AND fdi_type='I' ORDER BY sesam_date DESC limit 1";
	my $cmd_f = '. '.$sm_profile.' && '.$sm_db.' "'.$query_f.'"';
	my $cmd_d = '. '.$sm_profile.' && '.$sm_db.' "'.$query_d.'"';
	my $cmd_i = '. '.$sm_profile.' && '.$sm_db.' "'.$query_i.'"';

# check for full backups
	$DEBUG and print "DEBUG: cmd = $cmd_f\n";
	my $res_f = qx{$cmd_f};
	my ($tmp_date, $tmp_size, $date_d, $size_d, $date_f, $size_f, $date_i, $size_i);
	if ($res_f =~ /SUCCESS/) {
			$DEBUG and print "DEBUG:  Type: Full\n";
		if ($res_f =~ /MSG=1/) {
			my @fields = split('\|', $res_f);
			($tmp_date, $date_f) = split('=', $fields[2]);
			$DEBUG and print "DEBUG:   Date: $date_f\n";
			($tmp_size, $size_f) = split('=', $fields[3]);
			$DEBUG and print "DEBUG:   Size: $size_f\n";
			print "  Type: FULL - Date: $date_f - Size: $size_f\n";
			my $upd_json = '{"fqdn": "'.$server->{fqdn}.'", "backup_last_full_run": "'.$date_f.'", "backup_last_full_size": "'.$size_f.'"}';
			$DEBUG and print "DEBUG: JSON = $upd_json\n";
			my $upd_cmd = $curl.' -s -k -g -X PUT -H "Content-Type: application/json" -d \''.$upd_json.'\' '.$idb_url.'?idb_api_token='.$idb_api_token;
			$DEBUG and print "DEBUG: cmd = $upd_cmd\n";
			qx{$upd_cmd};
		} else {
			print "  Type: FULL - No Backups found!\n";
		}
	}

# check for differential backups
	$DEBUG and print "DEBUG: cmd = $cmd_d\n";
	my $res_d = qx{$cmd_d};
	if ($res_d =~ /SUCCESS/) {
		$DEBUG and print "DEBUG:  Type: Diff\n";
		if ($res_d =~ /MSG=1/) {
			my @fields = split('\|', $res_d);
			($tmp_date, $date_d) = split('=', $fields[2]);
			$DEBUG and print "DEBUG:   Date: $date_d\n";
			($tmp_size, $size_d) = split('=', $fields[3]);
			$DEBUG and print "DEBUG:   Size: $size_d\n";
			print "  Type: DIFF - Date: $date_d - Size: $size_d\n";
			my $upd_json = '{"fqdn": "'.$server->{fqdn}.'", "backup_last_diff_run": "'.$date_d.'", "backup_last_diff_size": "'.$size_d.'"}';
			$DEBUG and print "DEBUG: JSON = $upd_json\n";
			my $upd_cmd = $curl.' -s -k -g -X PUT -H "Content-Type: application/json" -d \''.$upd_json.'\' '.$idb_url.'?idb_api_token='.$idb_api_token;
			$DEBUG and print "DEBUG: cmd = $upd_cmd\n";
			qx{$upd_cmd};
		} else {
			print "  Type: DIFF - No Backups found!\n";
		}
	}

# check for incremental backups
	$DEBUG and print "DEBUG: cmd = $cmd_i\n";
	my $res_i = qx{$cmd_i};
	if ($res_i =~ /SUCCESS/) {
		$DEBUG and print "DEBUG:  Type: Inc\n";
		if ($res_i =~ /MSG=1/) {
			my @fields = split('\|', $res_i);
			($tmp_date, $date_i) = split('=', $fields[2]);
			$DEBUG and print "DEBUG:   Date: $date_i\n";
			($tmp_size, $size_i) = split('=', $fields[3]);
			$DEBUG and print "DEBUG:   Size: $size_i\n";
			print "  Type: INC  - Date: $date_i - Size: $size_i\n";
			my $upd_json = '{"fqdn": "'.$server->{fqdn}.'", "backup_last_inc_run": "'.$date_i.'", "backup_last_inc_size": "'.$size_i.'"}';
			$DEBUG and print "DEBUG: JSON = $upd_json\n";
			my $upd_cmd = $curl.' -s -k -g -X PUT -H "Content-Type: application/json" -d \''.$upd_json.'\' '.$idb_url.'?idb_api_token='.$idb_api_token;
			$DEBUG and print "DEBUG: cmd = $upd_cmd\n";
			qx{$upd_cmd};
		} else {
			print "  Type: INC  - No Backups found!\n";
		}
	}

	print "\n";
}
print "Generated $now\n";

## Main end
###################


###################
## Subs start

# print help
sub help {
	print "  usage: $0 [-d]\n\n";
	print "  Options:\n";
	print "\t-d\t\tenable debug output\n\n";
	exit 0;
}

## Subs end
###################
