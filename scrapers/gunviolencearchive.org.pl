#!/usr/bin/perl

use strict;
use warnings;

use Time::Local;

my $mqtt_broker = "localhost";
my $mqtt_user = "scraper-gva";
my $mqtt_pass = "dCvbtDx^C38";
#my $mqtt_auth = "-u '$mqtt_user' -P '$mqtt_pass'"
my $mqtt_auth = "";
my $mqtt_port = "1884";
my $site = "http://www.gunviolencearchive.org";
my $report = "reports/mass-shooting";

my $now = time;

my %months = (
	"January" => 0,
	"February" => 1,
	"March" => 2,
	"April" => 3,
	"May" => 4,
	"June" => 5,
	"July" => 6,
	"August" => 7,
	"September" => 8,
	"October" => 9,
	"November" => 10,
	"December" => 11,
);

sub encode_json {
  my $hashref = shift;
  my @ret;
  foreach(keys(%$hashref)) {
    push(@ret, "\"" . $_ . "\":\"" . $hashref->{$_} . "\"");
  }
  return "{" . join(',', @ret) . "}";
}

my @page = `wget $site/$report -O -`;
foreach (@page) {
  if (m#<tr class="(even|odd)"><td>([^<]+)</td>.*?<a href="/([^"]+)">View Incident</a>#) {
    my $date = $2; # formatted like January 31, 2016
    my $link = $3;

    if ($date =~ m/(\w+) (\d+), (\d+)/) {
      my $month = $1;
      my $day= $2;
      my $year = $3;

      print "date: $day.$month.$year\n";

      my $dt = timelocal(undef, undef, undef, $day, $months{$month}, $year);
print "now: $now, dt: $dt\n";
      my $seconds = $now - $dt;
      my $days = int($seconds / 3600 / 24);

      my %last_shooting = (
        ts => $now,
        days => $days,
        link => "$site/$link",
      );

      my $json_string = &encode_json(\%last_shooting);

      system("mosquitto_pub -r -i gva_scraper-$$ -t 'WEB/last_mass_shooting' -m '$json_string' -h $mqtt_broker -p $mqtt_port $mqtt_auth");
      last;
    }
  }
}
