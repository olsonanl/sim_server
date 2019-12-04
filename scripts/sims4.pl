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

use lib '/home/olson/SimServer/perlstuff/lib';

use Carp;
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

our $fig = "/disks/dserv/SimServer/FIGdisk";
#our $fig = "/disks/space0/fig/FIGdisk.anno_v5";
#our $fig = "/Volumes/raid2/FIGdisk.anno_v5";
our $data = "$fig/FIG/Data";
our $dbms            = "mysql";
our $db              = "sim_server";
our $dbuser          = "root";
our $dbpass          = "";
our $dbport          = 3306;
our $dbsock = "/tmp/mysql.sock";

our $simserv_host = "localhost";
our $simserv_port = 12111;
our $simserv_sock;

our $FileCache ;

use lib '/disks/space0/fig/SimServer/perl';

my $cgi = new CGI;

warn "start $simserv_sock\n";
reconnect();

sub reconnect
{
    for my $retry (1..10)
    {
	warn "connect to  $simserv_port\n";
	$simserv_sock = IO::Socket::INET->new(PeerAddr => $simserv_host,
					      PeerPort => $simserv_port,
					      Proto => 'tcp');
	if ($simserv_sock)
	{
	    warn "connected, sock=$simserv_sock\n";
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
    my %file_table;
    $FileCache = {};

    #
    # Load the file table and cache handles to the sims files found there.
    #
    my $dbh = connect_db();
    load_file_table($dbh, \%file_table, $FileCache, $fig);
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
    #$dbh = DBI->connect("DBI:mysql:dbname=$db", $dbuser, $dbpass);
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

    #
    # Find my entry.
    #

    my @me = grep { $_->[0] eq $id } @$syns;
    #print STDERR Dumper(\@me, $syns);

#    my $fcache = $GlobalCache->{file_cache};
    my $fcache = $FileCache;

    my @raw_sims = get_raw_sims($seeks, $fcache, $rep_id, $maxP, $filters);
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

sub load_file_table
{
    my($dbh, $file_table, $fcache, $fig) = @_;
    
    my $sth = $dbh->prepare("select file, fileno from file_table");
    $sth->execute;
    while (my $row = $sth->fetchrow_arrayref())
    {
	my($file, $num) = @$row;

	if ($file !~ m,^/,)
	{
	    $file = "$fig/$file";
	}
	
	$file_table->{$num} = $file;

	if ($file =~ m,Sims, and !exists($fcache->{$num}))
	{
	    my $fh = new FileHandle($file);
	    if ($fh)
	    {
		#warn "Opened $num $file\n";
		$fcache->{$num} = $fh;
	    }
	    else
	    {
		warn "Cannot open $file: $!";
	    }
	}
    }

}

#
# Retrive the mapping for an identifier. Returns
# the principal synonym, followed by the list of pairs of (syn, len). The list
# includes the principal identifier, so you can use this as
#
#  my ($rep, $syns, $seeks) = get_mapping($id);
#

sub get_mapping
{
    my($id, $simserv_sock) = @_;

    if (!print $simserv_sock "$id\n")
    {
	warn "error writing to sock: $!\n";
	$simserv_sock = &reconnect();
	print $simserv_sock "$id\n";
    }
    my $syns = [];
    my $seeks = [];

    while (<$simserv_sock>)
    {
	last if m,^//,;
	chomp;
	if (/^(\S+)\t(\d+)/)
	{
	    push(@$syns, [$1, $2]);
	}
    }

    while (<$simserv_sock>)
    {
	last if m,^//,;
	chomp;
	if (/^(\d+)\t(\d+)\t(\d+)/)
	{
	    push(@$seeks, [$1, $2, $3]);
	}
    }

    if (@$syns == 0)
    {
	#
	# It's a singleton. We don't know the length, but we
	# shouldn't need it, as it's a singleton.
	#

	@$syns = ([$id, 0]);
    }

    my $rep = $syns->[0]->[0];
    return($rep, $syns, $seeks);
}

sub get_raw_sims {
    my ($seeks, $fcache, $rep_id, $maxP, $filters ) = @_;
    my ( $sim_chunk, $seek, $fileN, $ln, $fh, $file, @lines, $sim );

    #  Set up behavior defaults (pretty wide open):

    my ( $show_env, $min_sim, $sim_meas, $min_q_cov, $min_s_cov, $sort_by );
    if ( $filters && ref( $filters ) eq "HASH" )
    {
        defined( $filters->{ maxP }      ) and $maxP      = $filters->{ maxP };
        defined( $filters->{ show_env }  ) and $show_env  = $filters->{ show_env };
        defined( $filters->{ min_sim }   ) and $min_sim   = $filters->{ min_sim };
        defined( $filters->{ sim_meas }  ) and $sim_meas  = $filters->{ sim_meas };
        defined( $filters->{ min_q_cov } ) and $min_q_cov = $filters->{ min_q_cov };
        defined( $filters->{ min_s_cov } ) and $min_s_cov = $filters->{ min_s_cov };
        defined( $filters->{ sort_by }   ) and $sort_by   = $filters->{ sort_by };
    }
    defined( $maxP )      or $maxP       =    10;
    defined( $show_env )  or $show_env   =     1;
    defined( $min_sim )   or $min_sim    =     0;
    defined( $sim_meas )  or $sim_meas   =   'id';
    defined( $min_q_cov ) or $min_q_cov  =     0;
    defined( $min_s_cov ) or $min_s_cov  =     0;
    defined( $sort_by )   or $sort_by = 'bits';

    foreach $sim_chunk ( @$seeks)
    {
        ( $fileN, $seek, $ln ) = @$sim_chunk;
        $fh = $fcache->{$fileN};
	
        $fh or confess "could not find sims for $fileN";

        #  Read file, parse lines, sanity check values, and filter E-value
        #   0.  The query peg
        #   1.  The similar peg
        #   2.  The percent id
        #   3.  Alignment length
        #   4.  Mismatches
        #   5.  Gap openings
        #   6.  The start of the match in the query peg
        #   7.  The end of the match in the query peg
        #   8.  The start of the match in the similar peg
        #   9.  The end of the match in the similar peg
        #  10.  E-value
        #  11.  Bit score
        #  12.  Length of query peg
        #  13.  Length of similar peg
        #  14.  Method

        push @lines, grep { ( @$_ >= 15 ) &&
                            ( $_->[10] =~ /^[0-9.e-]+$/ ) &&  # E-value
                            ( $_->[10] <= $maxP )   &&        # E-value test
                            ( $_->[11] =~ /^[0-9.]+$/ ) &&    # bit score
                            ( $_->[12] =~ /^\d+$/ ) &&        # query len
                            ( $_->[13] =~ /^\d+$/ ) &&        # subj len
                            ( $_->[6]  =~ /^\d+$/ ) &&        # q-match start
                            ( $_->[7]  =~ /^\d+$/ ) &&        # q-match end
                            ( $_->[8]  =~ /^\d+$/ ) &&        # s-match start
                            ( $_->[9]  =~ /^\d+$/ ) &&        # s-match end
                            ( $_->[2]  =~ /^[0-9.]+$/ )       # percent id
                          }
                     map  { [ split( /\t/, $_ ), "blastp" ] }
                     @{ read_block( $fh, $seek, $ln-1 ) };
    }

    my @linesS = sort { $a->[10] <=> $b->[10] } @lines;  # now sort and remove duplicates
    @lines = ();
    foreach $_ (@linesS)
    {
	if ((@lines == 0) || ($lines[$#lines]->[0] ne $_->[0]) || ($lines[$#lines]->[1] ne $_->[1]))
	{
	    push(@lines,$_);
	}
    }

    #  Similarity filter

    if ( $min_sim > 0 )
    {
        if    ( $sim_meas eq 'id'  )
        {
            @lines = grep { $_->[2] >= $min_sim } @lines;
        }
        elsif ( $sim_meas eq 'bpp' )
        {
            @lines = grep { $_->[11] >= $min_sim * ( $_->[7] - $_->[6] + 1 ) } @lines;
        }
    }

    #  Query coverage filter

    if ( $min_q_cov > 0 )
    {
        my $thresh = 0.01 * $min_q_cov;
        @lines = grep { ( abs( $_->[7] - $_->[6] ) + 1 ) >= ( $thresh * $_->[12] ) } @lines;
    }

    #  Subject coverage filter

    if ( $min_s_cov > 0 )
    {
        my $thresh = 0.01 * $min_s_cov;
        @lines = grep { ( abs( $_->[9] - $_->[8] ) + 1 ) >= ( $thresh * $_->[13] ) } @lines;
    }

    #  Order the surviving raw sims by requested criterion:

    if    ( $sort_by eq 'id' )                        # Percent identity
    {
        @lines = sort { $b->[2] <=> $a->[2] } @lines;
    }

    elsif ( $sort_by eq 'id2' )                       # Percent identity adjusted
    {
        #  Lower percent identity by 2 standard deviations to prevent random
        #  fluctuation in short sequences from moving them up so often.

        my ( $p, $len, $sigma );
        @lines = map  { $_->[0] }
                 sort { $b->[1] <=> $a->[1] }
                 map  { $p = 0.01 * $_->[2];                 # fraction identity
                        $len = abs( $_->[7] - $_->[6] ) + 1; # seq len
                        $sigma = sqrt( $p * ( 1 - $p ) / $len ); # binomial sigma
                        [ $_, $_->[2] - 200 * $sigma ]
                      }
                 @lines;
    }

    elsif ( $sort_by eq 'bpp' )                       # Bits per position
    {
        @lines = map  { $_->[0] }
                 sort { $b->[1] <=> $a->[1] }
                 map  { [ $_, $_->[11] / abs( $_->[7] - $_->[6] ) ] }
                 @lines;
    }

    elsif ( $sort_by eq 'bpp2' )                      # Bits per position adjusted
    {
        #  Lower score by 2 standard deviations to prevent random
        #  fluctuation in short sequences from moving them up so often.

        my ( $bpp, $len, $sigma );
        @lines = map  { $_->[0] }
                 sort { $b->[1] <=> $a->[1] }
                 map  { $len = abs( $_->[7] - $_->[6] ) + 1; # seq len
                        $bpp = $_->[11] / $len;              # bit per pos
                        $sigma = 2.5 * sqrt( 1 / $len );  # simple estimate
                        [ $_, $bpp - 2 * $sigma ]
                      }
                 @lines;
    }

    else                                              # Bit score (bits)
    {
        @lines = sort { $b->[11] <=> $a->[11] } @lines;
    }

    #  Bless the raw sims:

    return map { bless( $_, 'Sim' ); $_ } @lines;
}

sub expand_raw_sims {
    my( $simserv_sock, $raw_sims, $maxN, $maxP, $select, $dups, $max_expand, $filters ) = @_;
    my( $sim, $id1, $id2, %others, $x );

    #  Set up behavior defaults (pretty wide open):

    my ( $show_env );
    if ( $filters && ref( $filters ) eq "HASH" )
    {
        defined( $filters->{ maxN }       ) and $maxN       = $filters->{ maxN };
        defined( $filters->{ select }     ) and $select     = $filters->{ select };
        defined( $filters->{ max_expand } ) and $max_expand = $filters->{ max_expand };
        defined( $filters->{ show_env }   ) and $show_env   = $filters->{ show_env };
        defined( $filters->{ dups }       ) and $dups       = $filters->{ dups };
    }
    defined( $maxN )       or $maxN       = 1000000;   # Unlimited sims
    defined( $select )     or $select     =    'all';  # Show all expansions
    defined( $max_expand ) or $max_expand =       0;   # But none by default
    defined( $show_env )   or $show_env   =       1;   # Show environmental by default

    $max_expand = 1000000000 if ( $select =~ /^figx/ ); # figx forces unlimited expand

    my @sims = ();
    foreach $sim ( @$raw_sims )
    {
#	print STDERR "expand @$sim\n";
        $id2 = $sim->id2;
        if ( ! $dups )
        {
            next if $others{ $id2 };
            $others{ $id2 } = 1;
        }

        $id1 = $sim->id1;
        if ( ( $select eq "raw" ) || ( $max_expand <= 0 ) )
        {
            next if ( ! $show_env && ( $id2 =~ /^fig\|9999999/ ) );
            next if ( $id1 eq $id2 );
            push( @sims, $sim );
            return @sims if ( @sims >= $maxN );
        }
        else
        {
            my @relevant = ();
            $max_expand--;

	    #
	    # If we are expanding, determine the set of proteins that
	    # are equivalent to the protein that we are similar to.
	    #
	    # Depending on the options passed in, we filter the
	    # equivalent proteins found.
	    #

	    my($rep, $maps_to) = get_mapping($id2, $simserv_sock);

	    if (!$maps_to or !@$maps_to )
	    {
		@$maps_to = ([$id2, 0]);
	    }
	    defined( $maps_to->[0]->[1] ) || confess "maps_to";
	    my $rep_len = $maps_to->[0]->[1];
	    
	    my @maps_to = grep { $_->[0] !~ /^xxx\d+/ } @$maps_to;
	    
            if ( $select =~ /^figx?$/ )          # Only fig
            {
                @relevant = grep { $_->[0] =~ /^fig/ } @maps_to;
            }
            elsif ( $select =~ /^figx?_?pref/ )  # FIG preferred
            {
                @relevant = grep { $_->[0] =~ /^fig/ } @maps_to;
                if ( ! @relevant and $id2 !~ /^xxx\d+$/)
                {
                    push @sims, $sim;
                    return @sims if ( @sims >= $maxN );
                    next;
                }
            }
            elsif ( $select =~ /^ext/i )         # Not fig
            {
                @relevant = grep { $_->[0] !~ /^fig/ } @maps_to;
            }
            else                                 # All
            {
                @relevant = @maps_to;
            }

	    #
	    # Include the relevant sims.
	    #

            foreach $x ( @relevant )
            {
                my ( $x_id, $x_ln ) = @$x;
                defined( $x_ln ) || confess "x_ln id2='$id2' x_id='$x_id'";
                next if ( ! $show_env && ( $x_id =~ /^fig\|9999999/ ) );
                next if ( $id1 eq $x_id );

                my $delta2  = $rep_len - $x_ln;   # Coordinate shift

#		print STDERR "Expanding $id1 $id2 to $x_id $x_ln delta2=$delta2", Dumper(@maps_to);
		
                my $sim1    = [ @$sim ];                  # Make a copy
                $sim1->[1]  = $x_id;
                $sim1->[8] -= $delta2;
                $sim1->[9] -= $delta2;
                bless( $sim1, "Sim" );
                push( @sims, $sim1 );
                return @sims if ( @sims >= $maxN );
            }
        }
    }

    return @sims;
}



sub read_block
{
    my($fh,$seek,$ln) = @_;
    my($piece,$readN);

    seek($fh,$seek,0);
    my @lines = ();

    $readN = read($fh,$piece,$ln);
    ($readN == $ln)
            || confess "could not read the block of sims at $seek for $ln characters; $readN actually read";
    return [ split( /\n/, $piece ) ];
}


1;