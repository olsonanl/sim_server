#!/usr/bin/perl
use strict;
use DBI;
use CGI;

#
# PCH server code.
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

#
# bio* setup
#

#$fig = "/vol/seed-anno-mirror";
#$db = "fig_anno_v5";
#$dbuser = "seed";
#$dbhost = "biosql.mcs.anl.gov";
#undef $dbsock;

our $data = "$fig/FIG/Data";

my $dbh;

#$dbh = DBI->connect("DBI:mysql:dbname=$db;host=$dbhost;port=$dbport", $dbuser, $dbpass);
$dbh = DBI->connect("DBI:mysql:dbname=$db;mysql_socket=$dbsock", $dbuser, $dbpass);

$dbh or die "Could not open database: " . DBI->errstr;

my $cgi = new CGI;

my $func = $cgi->param('function');

if ($func eq 'coupled_to')
{
    my $id = $cgi->param('id1');
    $id or myerror($cgi, "500 missing id", "coupled_to missing id1 argument");
    do_coupled_to($cgi, $dbh, $id);
}
elsif ($func eq 'coupled_to_batch')
{
    my @id = $cgi->param('id1');
    @id or myerror($cgi, "500 missing id", "coupled_to missing id1 argument");
    do_coupled_to_batch($cgi, $dbh, \@id);
}
elsif ($func eq 'coupling_evidence')
{
    my $id1 = $cgi->param('id1');
    my $id2 = $cgi->param('id2');
    $id1 ne '' or myerror($cgi, "500 missing id1", "coupling_evidence missing id1 argument");
    $id2 ne '' or myerror($cgi, "500 missing id2", "coupling_evidence missing id2 argument");
    do_coupling_evidence($cgi, $dbh, $id1, $id2);
}
elsif ($func eq 'coupling_and_evidence')
{
    my $id = $cgi->param('id1');
    $id ne '' or myerror($cgi, "500 missing id", "coupling_and_evidence missing id1 argument");
    do_coupling_and_evidence($cgi, $dbh, $id);
}
elsif ($func eq 'coupling_and_evidence_batch')
{
    my @id_list = $cgi->param('id1');
    @id_list > 0 or myerror($cgi, "500 missing id", "coupling_and_evidence missing id1 argument");
    do_coupling_and_evidence_batch($cgi, $dbh, \@id_list);
}
elsif ($func eq 'in_pch_pin_with_and_evidence')
{
    my $id = $cgi->param('id1');
    $id ne '' or myerror($cgi, "500 missing id", "in_pch_pin_with_and_evidence missing id1 argument");
    do_in_pch_pin_with_and_evidence($cgi, $dbh, $id);
}
else
{
    myerror($cgi, "500 invalid function", "missing or invalid function");
}
exit;

sub do_coupled_to
{
    my($cgi, $dbh, $id) = @_;

    my $sth = $dbh->prepare(qq(SELECT peg2, score
			       FROM fc_pegs
			       WHERE peg1 = ?));
    $sth->execute($id);

    print $cgi->header('text/plain');
    
    while (my $row = $sth->fetchrow_arrayref())
    {
	#print STDERR join("\t", @$row), "\n";
	print join("\t", @$row), "\n";
    }
}

sub do_coupled_to_batch
{
    my($cgi, $dbh, $id_list) = @_;

    return unless @$id_list;
    my $cond = join(", ", map { "'$_'" } @$id_list);
    my $sth = $dbh->prepare(qq(SELECT peg1, peg2, score
			       FROM fc_pegs
			       WHERE peg1 in ($cond)));
    $sth->execute();

    print $cgi->header('text/plain');
    
    while (my $row = $sth->fetchrow_arrayref())
    {
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
	#print STDERR join("\t", @$row), "\n";
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
	#print STDERR "$score\t$peg2\n";
	print "$score\t$peg2";
	$ev_sth->execute($id, $peg2);
	
	while (my $res = $ev_sth->fetchrow_arrayref())
	{
	    print "\t" . join("\t", @$res);
	}
	print "\n";
    }
}

sub do_coupling_and_evidence_batch
{
    my($cgi, $dbh, $id_list) = @_;

    my $cond = join(", ", map { "'$_'" } @$id_list);
    my $sth = $dbh->prepare(qq(SELECT peg1, peg2, score
			       FROM fc_pegs
			       WHERE peg1 in ($cond)));
    my $ev_sth = $dbh->prepare(qq(SELECT peg3, peg4
				  FROM pchs
				  WHERE peg1 = ? AND peg2 = ?));
    $sth->execute();

    print $cgi->header('text/plain');
    
    while (my($peg1, $peg2, $score) = $sth->fetchrow_array())
    {
	#print STDERR "$score\t$peg2\n";
	print "$peg1\t$score\t$peg2";
	$ev_sth->execute($peg1, $peg2);
	
	while (my $res = $ev_sth->fetchrow_arrayref())
	{
	    print "\t" . join("\t", @$res);
	}
	print "\n";
    }
}

sub do_in_pch_pin_with_and_evidence
{
    my($cgi, $dbh, $id) = @_;
    my $sth = $dbh->prepare(qq(SELECT peg3, max(rep)
					  FROM pchs
					  WHERE peg1 = ?
					  GROUP BY peg3));
    $sth->execute($id);
    print $cgi->header('text/plain');
    while (my $r = $sth->fetchrow_arrayref())
    {
	print "$r->[0]\t$r->[1]\n";
    }

}

sub myerror
{
    my($cgi, $stat, $msg) = @_;
    print $cgi->header(-status =>  $stat);
    
    print "$msg\n";
    exit;
}
