package CsvSelect::File;

use strict;
use warnings;

use IO::File;
use Text::CSV;

use CsvSelect::ResultSet;

sub resultset {
    my($class, $file, $fileno) = @_;

    my $fh = IO::File->new($file)
                || die "Can't open $file: $!";

    my $csvparser = Text::CSV->new({ binary => 1, auto_diag => 1 });
    my @headers = $csvparser->header($fh);

    my @rows;
    while( my $row = $csvparser->getline($fh) ) {
        push @rows, $row;
    }

    my $rs = CsvSelect::ResultSet->new(\@rows);
    for (my $i = 0; $i < @headers; $i++) {
        $rs->add_column_name($i, $headers[$i]);
        my $letter = ('A' .. 'Z')[$i];
        $rs->add_column_name($i, "${fileno}:$letter");
    }
    return $rs;
}

1;
