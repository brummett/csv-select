package CsvSelect::ResultSet;

use strict;
use warnings;

use Carp qw(croak);
use Text::CSV;

use CsvSelect::Where;

sub new {
    my($class, $rows, $column_names) = @_;

    unless ($rows and ref($rows) eq 'ARRAY') {
        croak('$rows must be a listref');
    }
    if (defined($column_names) and ref($column_names) ne 'ARRAY') {
        croak('$column_names must be a listref');
    }
    my $self = {
        rows => $rows,
        column_names => $column_names || [],
    };
    return bless($self, $class);
}


sub rows {
    return shift->{rows};
}

sub add_column_name {
    my($self, $idx, $name) = @_;
    my $list = $self->_column_names->[$idx] ||= [];
    push @$list, $name;
}

sub _column_names {
    return shift->{column_names}
}

# The convention is that the first name/alias for each column is the "name"
sub headers {
    my $self = shift;
    my @headers = map { $_->[0] } @{ $self->_column_names };
    return @headers;
}

sub name_for_column_idx {
    my($self, $idx) = @_;
    return $self->_column_names->[$idx]->[0];
}

sub column_idx_for_name {
    my($self, $name) = @_;

    $name = uc($name);
    for (my $idx = 0; $idx < @{ $self->_column_names }; $idx++) {
        foreach my $alias ( @{ $self->_column_names->[$idx] } ) {
            return $idx if $alias eq $name;
        }
    }
    return;
}

sub foreach {
    my($self, $code) = @_;

    my $rows = $self->rows;
    for (my $i = 0; $i < @$rows; $i++) {
        $code->($rows->[$i], $i);
    }
}

sub width {
    my $self = shift;
    return scalar(@{$self->rows->[0]});
}

sub count {
    my $self = shift;
    scalar(@{$self->rows});
}

sub get_row {
    my($self, $i) = @_;
    return $self->rows->[$i];
}

sub inner_join {
    my($self, $join_clauses, @resultsets) = @_;

    my @joins = $self->_parse_join_clauses($join_clauses, @resultsets);
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

    my @new_column_names = ( @{ $self->_column_names }, @{ $next_resultset->_column_names } );
    my $new_resultset = __PACKAGE__->new(\@rows, \@new_column_names);

    return $new_resultset->_inner_join($join_count+1, $joins, @resultsets);
}


sub outer_join {
    my($self, $join_clauses, @resultsets) = @_;

    my @joins = $self->_parse_join_clauses($join_clauses, @resultsets);
    $self->_outer_join(1, \@joins, 1, 1, @resultsets);
}

sub left_join {
    my($self, $join_clauses, @resultsets) = @_;

    my @joins = $self->_parse_join_clauses($join_clauses, @resultsets);
    $self->_outer_join(1, \@joins, 1, undef, @resultsets);
}

sub right_join {
    my($self, $join_clauses, @resultsets) = @_;

    my @joins = $self->_parse_join_clauses($join_clauses, @resultsets);
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

    my @new_column_names = ( @{ $self->_column_names }, @{ $next_resultset->_column_names } );
    my $new_resultset = __PACKAGE__->new(\@rows, \@new_column_names);

    return $new_resultset->_inner_join($join_count+1, $joins, @resultsets);
}


sub _parse_join_clauses {
    my($self, $join_exprs, @resultsets) = @_;

    my $operators = qr(=|<|>|<=|>=);

    my @joins;
    foreach my $join_clause ( @$join_exprs ) {
        my($left_fileno, $left_column, $operator, $right_fileno, $right_column) = $join_clause =~ m/(\d+):(\w)($operators)(\d+):(\w)/;
        unless (defined $left_fileno) {
            croak "Can't parse join: $join_clause";
        }

        ($left_column, $right_column) = (uc($left_column), uc($right_column));
        my $left_join_expr = "${left_fileno}:${left_column}";
        my $left_idx = $self->column_idx_for_name($left_join_expr);
        croak "Unknown column named in join: $left_join_expr" unless (defined $left_idx);

        my $right_join_expr = "${right_fileno}:${right_column}";
        my $right_idx = $resultsets[0]->column_idx_for_name($right_join_expr);
        croak "Unknown column named in join: $right_join_expr" unless (defined $right_idx);

        my $join = CsvSelect::Where->new("${left_fileno}:${left_idx}", $operator, "${right_fileno}:${right_idx}");
        my $join_idx = _max_fileno($left_fileno, $right_fileno) - 1; # -1 because it's 0-based
        $joins[$join_idx] ||= [];
        push @{ $joins[$join_idx] }, $join;
    }

    return @joins;
}

sub _parse_select_clauses {
    my($self, $select_exprs, @resultsets) = @_;

    my @selects = split(',', $select_exprs);

    my @columns_to_extract = map {
            $self->column_idx_for_name($_)
        } @selects;

    unless (@columns_to_extract) {
        # No select clause, return everything
        for (my $i = 0; $i < $self->width(); $i++) {
            $columns_to_extract[$i] = $i;
        }
    }

    return @columns_to_extract;
}

sub select {
    my($self, $select_exprs) = @_;

    my @columns_to_show = $self->_parse_select_clauses($select_exprs);

    my @rows;
    $self->foreach(sub {
        my $row = shift;
        push @rows, [ @$row[@columns_to_show] ];
    });

    my @original_columns = @{ $self->_column_names };
    my @column_names = @original_columns[@columns_to_show];

    __PACKAGE__->new(\@rows, \@column_names);
}

sub _max_fileno {
    my($left, $right) = @_;

    return( $left > $right ? $left : $right );
}

sub write_to {
    my($self, $filename) = @_;

    my $fh = IO::File->new($filename, 'w')
                || die "Can't open $filename for writing: $!";
    my $csvparser = Text::CSV->new({ binary => 1, auto_diag => 1});

    $csvparser->say($fh, [ $self->headers ] );
    $self->foreach(sub {
        my $row = shift;
        $csvparser->say($fh, $row);
    });
}

1;

