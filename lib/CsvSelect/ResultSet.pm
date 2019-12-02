package CsvSelect::ResultSet;

use strict;
use warnings;

use Carp qw(croak);

use CsvSelect::Where;

sub new {
    my($class, $rows) = @_;

    return bless($rows, $class);
}

sub foreach {
    my($self, $code) = @_;

    for (my $i = 0; $i < @$self; $i++) {
        $code->($self->[$i], $i);
    }
}

sub width {
    my $self = shift;
    return scalar(@{$self->[0]});
}

sub count {
    my $self = shift;
    scalar(@$self);
}

sub get_row {
    my($self, $i) = @_;
    return $self->[$i];
}

sub inner_join {
    my($self, $join_clauses, @resultsets) = @_;

    my @joins = $self->_parse_join_clauses($join_clauses);
    $self->_inner_join(1, \@joins, @resultsets);
}

sub _inner_join {
    my($self, $join_count, $joins, @resultsets) = @_;

    my $next_resultset = shift @resultsets;
    return $self unless $next_resultset;

    my @rows;
    $self->foreach(sub {
        my $left_row = shift;
        $next_resultset->foreach( sub {
            my $right_row = shift;

            foreach my $join ( @{ $joins->[$join_count] } ) {
                return unless $join->($left_row, $right_row);
            }
            push @rows, [ @$left_row, @$right_row ];
        });
    });

    my $new_resultset = __PACKAGE__->new(\@rows);
    return $new_resultset->_inner_join($join_count+1, $joins, @resultsets);
}


sub outer_join {
    my($self, $join_clauses, @resultsets) = @_;

    my @joins = $self->_parse_join_clauses($join_clauses);
    $self->_outer_join(1, \@joins, 1, 1, @resultsets);
}

sub left_join {
    my($self, $join_clauses, @resultsets) = @_;

    my @joins = $self->_parse_join_clauses($join_clauses);
    $self->_outer_join(1, \@joins, 1, undef, @resultsets);
}

sub right_join {
    my($self, $join_clauses, @resultsets) = @_;

    my @joins = $self->_parse_join_clauses($join_clauses);
    $self->_outer_join(1, \@joins, undef, 1, @resultsets);
}

sub _outer_join {
    my($self, $join_count, $joins, $is_leftjoin, $is_rightjoin, @resultsets) = @_;

    my $next_resultset = shift @resultsets;
    return $self unless $next_resultset;

    my @rows;  # Rows for the final resultset

    # Rownums in the next_resultset (right part of the join) that haven't
    # joined to anything yet
    my @right_rows_not_matching;
    for (my $i = 0; $i < $next_resultset->count; $i++) {
        $right_rows_not_matching[$i] = $i;
    }

    $self->foreach(sub {
        my $left_row = shift;

        my $left_row_matched;
        $next_resultset->foreach( sub {
            my $right_row = shift;
            my $right_row_idx = shift;

            foreach my $join ( @{ $joins->[$join_count] } ) {
                return unless ( $join->($left_row, $right_row) );
            }
            $left_row_matched = 1;
            push @rows, [ @$left_row, @$right_row ];
            $right_rows_not_matching[$right_row_idx] = undef;  # mark this row jas successfully joined to something
        });

        if ($is_leftjoin and !$left_row_matched) {
            push @rows, [ @$left_row, ('') x  $next_resultset->width()  ];
        }
    });

    if ($is_rightjoin) {
        my @left_row_nulls = ( '' ) x $self->width();
        push @rows,
            map { [ @left_row_nulls, @$_ ] }
            map { $next_resultset->get_row($_) }
            grep { defined } @right_rows_not_matching
        ;
    }

    my $new_resultset = __PACKAGE__->new(\@rows);
    return $new_resultset->_inner_join($join_count+1, $joins, @resultsets);
}


sub _parse_join_clauses {
    my($self, $join_exprs, @resultsets) = @_;

    my @column_adjustment = $self->resolve_column_indexes_for_final_resultset(@resultsets);

    my $operators = qr(=|<|>|<=|>=);

    my @joins;
    foreach my $join_clause ( @$join_exprs ) {
        my($left_fileno, $left_column, $operator, $right_fileno, $right_column) = $join_clause =~ m/(\d+):(\w)($operators)(\d+):(\w)/;
        unless (defined $left_fileno) {
            croak "Can't parse join: $join_clause";
        }

        $left_column = _xlate_column_letter_to_idx($left_column);
        $left_column += $column_adjustment[$left_fileno - 1];
        $right_column = _xlate_column_letter_to_idx($right_column);

        my $join = CsvSelect::Where->new("${left_fileno}:${left_column}", $operator, "${right_fileno}:${right_column}");
        my $join_idx = _max_fileno($left_fileno, $right_fileno) - 1; # -1 because it's 0-based
        $joins[$join_idx] ||= [];
        push @{ $joins[$join_idx] }, $join;
    }

    return @joins;
}


my %letters = ( (map { $_ => ord($_) - ord('A') } ( 'A' .. 'Z' )),
                (map { $_ => ord($_) - ord('a') } ( 'a' .. 'Z' )) );
sub _xlate_column_letter_to_idx {
    my $letter = shift;
    unless (exists $letters{$letter}) {
        croak "Unknown column name: $letter";
    }
    print "Column $letter is idx $letters{$letter}\n";
    return $letters{$letter};
}


sub _max_fileno {
    my($left, $right) = @_;

    return( $left > $right ? $left : $right );
}

# As we join more resultsets on the right, we need to shift the column indexes
# up to account for the columns already joined on the left
# This would probably be better solved by letting a ResultSet's columns have
# aliases that propogate to derived Resultsets - then we'd be able to refer
# to them by their name.
sub resolve_column_indexes_for_final_resultset {
    my($class, @resultsets) = @_;

    my @column_adjustment = ( 0 );
    for (my $i = 1; $i < @resultsets; $i++) {
        push @column_adjustment, $resultsets[$i-1]->width();
    }
    return @column_adjustment;
}

1;

