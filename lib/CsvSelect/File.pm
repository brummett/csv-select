package CsvSelect::File;

use strict;
use warnings;

use IO::File;
use Text::Csv;

use CsvSelect::ResultSet;

sub resultset {
    my($class, $file) = @_;

    my $fh = IO::File->new($file)
                || die "Can't open $file: $!";

    my $csvparser = Text::CSV->new({ binary => 1, auto_diag => 1 });
    $csvparser->header($fh);

    my @rows;
    while( my $row = $csvparser->getline($fh) ) {
        push @rows, $row;
    }

    return CsvSelect::ResultSet->new(\@rows);
}

1;
