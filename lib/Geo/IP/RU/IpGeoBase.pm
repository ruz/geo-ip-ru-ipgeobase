use 5.008;
use strict;
use warnings;

package Geo::IP::RU::IpGeoBase;

our $VERSION = '0.01';

=head1 NAME

Geo::IP::RU::IpGeoBase - look up location by IP address in Russia

=head1 DESCRIPTION

This module allows you to look up location in DB provided by
http://ipgeobase.ru service. Access to the DB is free. Contains
information about city, region, federal district and coordinates.

DB provided as plain text files and is not very suitable for look
ups without loading all data into memory. Instead it's been decided
to import data into a database. Use command line utility to create
and update back-end DB.

At this moment DB can be created in SQLite and mysql only. If
you create table manually then probably module will just work.
It's very easy to add support for more back-end DBs. Patches are
welcome.

=head1 METHODS

=head2 new

Returns a new object. Takes a hash with options, mostly
description of the back-end:

    Geo::IP::RU::IpGeoBase->new( db => {
        dbh => $dbh, table => 'my_table',
    } );
    # or
    Geo::IP::RU::IpGeoBase->new( db => {
        dsn => 'dbi:mysql:mydb',
        user => 'root', pass => 'secret',
        table => 'my_table',
    } );

=over 4

=item * dbh - connected L<DBI> handle, or you can use dsn.

=item * dsn, user, pass - DSN like described in L<DBI>, for
example 'dbi:SQLite:my.db', user name and his password.

=item * table - name of the table with data, default
is 'ip_geo_base_ru'.

=back

=cut

sub new {
    my $proto = shift;
    my $self = bless { @_ }, ref($proto) || $proto;
    return $self->init;
}

sub init {
    my $self = shift;

    die "No information about database"
        unless my $db = $self->{'db'};

    unless ( $db->{'dbh'} ) {
        die "No dsn and no dbh" unless $db->{'dsn'};

        require DBI;
        $db->{'dbh'} = DBI->connect(
            $db->{'dsn'}, $db->{'user'}, $db->{'pass'},
            { RaiseError => 1, PrintError => 0 }
        );
        $db->{'dbh'}->do("SET NAMES 'utf8'");
        $db->{'decode'} = 1;
    } else {
        $db->{'decode'} = 1
            unless exists $db->{'decode'};
    }
    if ( $db->{'decode'} ) {
        require Encode;
        $db->{'decoder'} = Encode::find_encoding('UTF-8');
    }

    $db->{'driver'} = $db->{'dbh'}{'Driver'}{'Name'}
        or die "Couldn't figure out driver name of the DB";

    $db->{'table'} ||= 'ip_geo_base_ru';
    $db->{'quoted_table'} = $db->{'dbh'}->quote_identifier($db->{'table'});

    return $self;
}

=head2 find_by_ip

Takes an IP in 'xxx.xxx.xxx.xxx' format and returns information
about blocks that contains this IP. Yep, blocks, not a block.
In theory DB may contain intersecting blocks.

Each record is a hash reference with the fields matching table
columns: istart, iend, start, end, city, region, federal_district,
latitude and longitude.

=cut

sub find_by_ip {
    my $self = shift;
    my $ip = $self->ip2int(shift);
    return $self->intersections( $ip, $ip, order => 'ASC', @_ );
}

sub ip2int { return unpack 'N', pack 'C4', split /[.]/, $_[1] }

sub intersections {
    my $self = shift;
    my ($istart, $iend, %rest) = @_;
    my $table = $self->db_info->{'quoted_table'};
    my $query = "SELECT * FROM $table WHERE istart <= ? AND iend >= ?";
    $query .= ' ORDER BY iend-istart '. $rest{'order'}
        if $rest{'order'};
    return @{ $self->decode( $self->dbh->selectall_arrayref(
        "SELECT * FROM $table WHERE istart <= ? AND iend >= ?",
        { Slice => {} }, $iend, $istart
    ) ) };
}

sub fetch_record {
    my $self = shift;
    my ($istart, $iend) = @_;
    my $table = $self->db_info->{'quoted_table'};
    return $self->decode( $self->dbh->selectrow_hashref(
        "SELECT * FROM $table WHERE istart = ? AND iend = ?",
        undef, $istart, $iend
    ) );
}

sub insert_record {
    my $self = shift;
    my %rec  = @_;

    my $table = $self->db_info->{'quoted_table'};
    my @keys = keys %rec;
    return $self->dbh->do(
        "INSERT INTO $table(". join( ', ', @keys) .")"
        ." VALUES (". join( ', ', map "?", @keys) .")",
        undef, map $rec{$_}, @keys
    );
}

sub update_record {
    my $self = shift;
    my %rec  = @_;

    my $table = $self->db_info->{'quoted_table'};

    my ($istart, $iend) = delete @rec{'istart', 'iend'};
    my @keys = keys %rec;
    return $self->dbh->do(
        "UPDATE $table SET ". join( ' AND ', map "$_ = ?", @keys)
        ." WHERE istart = ? AND iend = ?",
        undef, ( map $rec{$_}, @keys ), $istart, $iend
    );
}

sub delete_record {
    my $self = shift;
    my ($istart, $iend) = @_;
    my $table = $self->db_info->{'quoted_table'};
    return $self->dbh->do(
        "DELETE FROM $table WHERE istart = ? AND iend = ?",
        undef, $istart, $iend
    );
}

sub decode {
    return unless $_[0]->{'db'}{'decode'};

    my $decoder = $_[0]->{'db'}{'decoder'};
    foreach my $r ( ref($_[1]) eq 'ARRAY'? @$_[1] : $_[1] ) {
        $_ = $decoder->decode($_) foreach values %$r;
    }
    return $_[1];
}

sub db_info { return $_[0]->{'db'} }

sub dbh { return $_[0]->{'db'}{'dbh'} }

sub create_table {
    my $self = shift;

    my $driver = $self->db_info->{'driver'};

    my $call = 'create_'. lc( $driver ) .'_table';
    die "Table creation is not supported for $driver"
        unless $self->can($call);

    return $self->$call();
}

sub create_sqlite_table {
    my $self = shift;

    my $table = $self->db_info->{'quoted_table'};
    my $query = <<END;
CREATE TABLE $table (
    istart INTEGER NOT NULL,
    iend INTEGER NOT NULL,
    start TEXT NOT NULL,
    end TEXT NOT NULL,
    status TEXT,
    city TEXT,
    region TEXT,
    federal_district TEXT,
    latitude REAL,
    longitude REAL,
    in_update INT NOT NULL DEFAULT(0),
    PRIMARY KEY (istart ASC, iend ASC)
)
END
    return $self->dbh->do($query);
}

sub create_mysql_table {
    my $self = shift;
    my $table = $self->db_info->{'quoted_table'};
    my $query = <<END;
CREATE TABLE $table (
    istart UNSIGNED INTEGER NOT NULL,
    iend UNSIGNED INTEGER NOT NULL,
    start VARCHAR(15) NOT NULL,
    end VARCHAR(15) NOT NULL,
    status VARCHAR(64),
    city TEXT,
    region TEXT,
    federal_district TEXT,
    latitude FLOAT(8,6),
    longitude FLOAT(8,6),
    in_update TINYINT NOT NULL DEFAULT(0),
    PRIMARY KEY (istart, iend)
) CHARACTER SET 'utf8'
END
    return $self->dbh->do($query);
}

=head1 AUTHOR

Ruslan Zakirov E<gt>Ruslan.Zakirov@gmail.comE<lt>

=head1 LICENSE

Under the same terms as perl itself.

=cut

1;
