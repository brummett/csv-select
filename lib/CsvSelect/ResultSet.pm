package CsvSelect::ResultSet;

use strict;
use warnings;

sub new {
    my($class, $rows) = @_;

    return bless($rows, $class);
}

sub foreach {
    my($self, $code) = @_;

    for (my $i = 0; $i < @$self; $i++) {
        $code->($self->[$i]);
    }
}

1;

