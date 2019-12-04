use strict;
use DBI qw(:sql_types);
use CGI;
my $cgi = new CGI;

#
# BBH server code.
# Keep this free of FIG references - it is intended to be run from mod_perl where
# we likely don't have a full SEED environment.
#

our $fig = "/disks/space0/vol/dserv/SimServer/FIGdisk";
#our $fig = "/Volumes/raid2/FIGdisk.anno_v5";
our $dbms            = "mysql";
our $db              = "sim_server";
our $dbuser          = "sim_server";
our $dbpass          = "";
our $dbport          = 3306;
our $dbsock = "/var/lib/mysql/mysql.sock";
our $dbhost;

my $table = "bbh_034";

#
# bio* setup
#
#if ($ENV{HTTP_HOST} =~ /bio/) {
#    $fig = "/vol/seed-anno-mirror";
#    $db = "fig_anno_v5";
#    $dbuser = "seed";
#    $dbhost = "biosql.mcs.anl.gov";
#    undef $dbsock;
#}

our $data = "$fig/FIG/Data";

my $dbh;

if ($dbhost ne '')
{
    $dbh = DBI->connect("DBI:mysql:dbname=$db;host=$dbhost;port=$dbport", $dbuser, $dbpass);
}
else
{
    $dbh = DBI->connect("DBI:mysql:dbname=$db;mysql_socket=$dbsock", $dbuser, $dbpass);
}

$dbh or die "Could not open database: " . DBI->errstr;



my $cutoff = $cgi->param('cutoff');
if ($cutoff eq '')
{
    $cutoff = 1.0e-10 + 0;
}


my $id = $cgi->param('id');

$id or myerror($cgi, "500 missing id", "bbhs missing id argument");

# Find out if we're doing a single PEG or a bunch. We're doing a bunch if
# there's a wild card in the PEG id.
my ($filter, $flds, $idx);
if ($id =~ /%/) {
    # Here we have a bunch.
    $filter = "peg1 LIKE ?";
    $flds = "peg1, peg2, psc, nsc";
    $idx = 1;
} else {
    $filter = "peg1 = ?";
    $flds = "peg2, psc, nsc";
    $idx = 0;
}

# See if we want to filter on target genomes.
my $targets = $cgi->param('targets');
my @targets = ();
if ($targets) {
    @targets = map { "fig|$_" } split /,/, $targets;
}

#
# Need "0+?" to force a numeric comparison, since psc is
# a varchar field.
#
my $sth = $dbh->prepare("SELECT $flds FROM $table WHERE $filter AND (psc + 0) < ? ORDER BY psc + 0, nsc DESC");

$sth->bind_param(1, $id);
$sth->bind_param(2, $cutoff,  SQL_REAL);
$sth->execute;

print $cgi->header('text/plain');

while (my $row = $sth->fetchrow_arrayref())
{
    # We need to see here if it's necessary to filter to target genomes. We get no index help
    # on this, so we do it here instead of in the query.
    my $ok = 1;
    if ($targets) {
	my $peg2 = $row->[$idx];
	$ok = scalar(grep { substr($peg2, 0, length($_)) eq $_ } @targets);
    }
    if ($ok) {
	print join("\t", @$row), "\n";
    }
}

sub do_coupling_evidence
{
    my($cgi, $dbh, $id1, $id2) = @_;

    my $sth = $dbh->prepare(qq(SELECT peg3, peg4, rep
			       FROM pchs
			       WHERE peg1 = ? AND peg2 = ?));
    $sth->execute($id1, $id2);

    print $cgi->header('text/plain');
    
    while (my $row = $sth->fetchrow_arrayref())
    {
	print join("\t", @$row), "\n";
    }
}

sub do_coupling_and_evidence
{
    my($cgi, $dbh, $id) = @_;

    my $sth = $dbh->prepare(qq(SELECT peg2, score
			       FROM fc_pegs
			       WHERE peg1 = ?));
    my $ev_sth = $dbh->prepare(qq(SELECT peg3, peg4
				  FROM pchs
				  WHERE peg1 = ? AND peg2 = ?));
    $sth->execute($id);

    print $cgi->header('text/plain');
    
    while (my($peg2, $score) = $sth->fetchrow_array())
    {
	print "$score\t$peg2";
	$ev_sth->execute($id, $peg2);
	
	while (my $res = $ev_sth->fetchrow_arrayref())
	{
	    print "\t" . join("\t", @$res);
	}
	print "\n";
    }
}

sub myerror
{
    my($cgi, $stat, $msg) = @_;
    print $cgi->header(-status =>  $stat);
    
    print "$msg\n";
    exit;
}
