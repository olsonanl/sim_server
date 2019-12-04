#
# Similarity server.
#
# Request parameters are
#
# id		requested peg
# maxN 		Maximum number of returned sims
# maxP		Maximum P-score
# select	Choose processing:
#	        "raw" means that the similarities will not be expanded (by far fastest option)
#	        "fig" means return only similarities to fig genes
#	        "all" means that you want all the expanded similarities.
#	        "figx" means exapand until the maximum number of fig sims
# max_expand
# filters
# 

#use lib '/home/olson/SimServer/perlstuff/lib';

use Data::Dumper;
use strict;
use CGI;
use DBI;
use FileHandle;
use IO::Socket::INET;

#
# Since this is mod_perl, we cache the peg mapping and sim seeks data.
#
# Since this is a standalone app, we hardcode where to find the database with the seeks information.
#

our $fig = "/disks/space0/vol/dserv-2/SimServer/FIGdisk";

#our $fig = "/disks/dserv/SimServer/FIGdisk";
#our $fig = "/disks/space0/fig/FIGdisk.anno_v5";
#our $fig = "/Volumes/raid2/FIGdisk.anno_v5";
our $data = "$fig/FIG/Data";
our $dbms            = "mysql";
our $db              = "sim_server";
our $dbuser          = "sim_server";
our $dbpass          = "";
our $dbport          = 3306;
our $dbsock = "/var/lib/mysql/mysql.sock";

our $simserv_host = "127.0.0.1";
#our $simserv_host = "bio-big.mcs.anl.gov";
#our $simserv_host = "cgat.mcs.anl.gov";
#our $simserv_port = 12112;
our $simserv_port = 12033;
our $simserv_sock;

our $FileCache;
our $FileTable;

use lib '/disks/space0/vol/dserv-2/SimServer/perl';
use SimServer3;

my $cgi = new CGI;

#warn "start $simserv_sock\n";
reconnect();

sub reconnect
{
    for my $retry (1..10)
    {
	#warn "connect to  $simserv_port\n";
	$simserv_sock = IO::Socket::INET->new(PeerAddr => $simserv_host,
					      PeerPort => $simserv_port,
					      Proto => 'tcp');
	if ($simserv_sock)
	{
	    #warn "connected, sock=$simserv_sock\n";
	    last;
	}
	else
	{
	    warn "Cannot connect to sim server at $simserv_host $simserv_port: $!\n";
	    sleep 2;
	}
    }
    if (!$simserv_sock)
    {
	&myerror($cgi, "500 no server", "cannot connect to cache server at $simserv_host/$simserv_port");
    }
    return $simserv_sock;
}



if (!defined($FileCache))
{
    $FileTable = {};
    $FileCache = {};

    #
    # Load the file table and cache handles to the sims files found there.
    #
    my $dbh = connect_db();
    load_file_table($dbh, $FileTable, $FileCache, $fig);
}

my @ids = $cgi->param('id');
my $maxN = $cgi->param('maxN');
my $maxP = $cgi->param('maxP');
my $select = $cgi->param('select');
my $max_expand = $cgi->param('max_expand');

#
# Check for filters.
#

my $filters = {};
for my $filt (grep { s/^filter_//; } $cgi->param())
{
    $filters->{$filt} = $cgi->param("filter_$filt");
}
#print STDERR "Got filters ", Dumper($filters);

#
# Defaults.
#

$max_expand = defined( $max_expand ) ? $max_expand : 10000;

if (@ids eq '')
{
    &myerror($cgi, "500 no id", "no id passed");
}

print $cgi->header('text/plain');

for my $id (@ids)
{
    &do_sims($cgi, $id, $maxN, $maxP, $select, $max_expand, $filters);
}

sub connect_db
{
    my $dbh;
    $dbh = DBI->connect("DBI:mysql:dbname=$db;mysql_socket=$dbsock", $dbuser, $dbpass);
    $dbh or die "Could not open database: " . DBI->errstr;
    return $dbh;
}

sub do_sims
{
    my ($cgi, $id, $maxN, $maxP, $select, $max_expand, $filters) = @_;

    my $dbh = connect_db();

    my($rep_id, $syns, $seeks) = get_mapping($id, $simserv_sock);
    #print STDERR "id '$id' maps to $rep_id<br>\n";
#print STDERR Dumper($seeks, $syns);

    #
    # Find my entry.
    #

    my @me = grep { $_->[0] eq $id } @$syns;
    #print STDERR Dumper(\@me, $syns);

#    my $fcache = $GlobalCache->{file_cache};
    my $fcache = $FileCache;

    my @raw_sims =  get_raw_sims($seeks, $FileTable, $fcache, $rep_id, $maxP, $filters);
    #my $n = @raw_sims;
    #print STDERR "Got $n raw sims for $rep_id\n";

    #  If the query is not the representative, make sims look like it is
    #  by replacing id1 and fixing match coordinates if lengths differ.

    my $delta = $syns->[0]->[1] - $me[0]->[1];
#    print STDERR "Delta=$delta\n";
    if ( $id ne $rep_id )
    {

        foreach my $sim ( @raw_sims )
        {
            $sim->[0]  = $id;
            $sim->[6] -= $delta;
            $sim->[7] -= $delta;
        }
    }

    #  The query must be present for expanding matches to identical sequences.

    if ( ( $max_expand > 0 ) && ( $select ne "raw" ) )
    {
        unshift( @raw_sims, bless( [ $id,
                                     $rep_id,
                                     "100.00",
                                     $me[0]->[1],
                                     0,
                                     0,
                                     1,        $me[0]->[1],
                                     $delta+1, $syns->[0]->[1],
                                     0.0,
                                     2 * $me[0]->[1],
                                     $me[0]->[1],
                                     $syns->[0]->[1],
                                     "blastp"
                                   ], 'Sim'
                                 )
               );
        $max_expand++;
    }

#    print STDERR "\n\n"; for ( @raw_sims ) { print STDERR join( ", ", @{ $_ } ), "\n" }

    #  expand_raw_sims now handles sanity checks on id1 eq id2 and id2
    #  is not deleted.  This lets it keep count of the actual number of
    #  sims reported!

    my @sims = expand_raw_sims($simserv_sock, \@raw_sims, $maxN, $maxP, $select, 1, $max_expand, $filters );

    map { print join("\t", @$_), "\n"; } @sims;
}


sub myerror
{
    my($cgi, $stat, $msg) = @_;
    print $cgi->header(-status =>  $stat);

    print "$msg\n";
    exit;
}
