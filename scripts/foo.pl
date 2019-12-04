print "Content-type: text/plain\n\n";
print "OOOO\n";
$cmd = "bash -c 'date'";

print "$cmd\n";
$out = `$cmd`;
print "out=$out\n";

use FileHandle;
$i = 0;
my @a;
while ($i < 10000)
{
my $x = new FileHandle("</dev/null");
push(@a, $x);
last unless ref($x);
	$i++;
	print "$x $i\n" if($i % 100 == 0);
}
print "i=$i\n";

