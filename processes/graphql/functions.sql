create or replace function graphql.search_parcels (search text)
    returns setof parcels as $$
    select * from graphql.parcels 
    where parcelno ilike ('%' || search || '%')
$$ language sql stable;

create or replace function graphql.search_parcels_latlng (lat double precision, lng double precision, radius integer)
    returns setof parcels as $$
    select
        parcelno,
        wkb_geometry,
        address,
        st_distance(
            st_transform(st_setsrid(st_makepoint(lng, lat), 4326), 2898), 
            st_centroid(wkb_geometry)) 
        as distance 
    from graphql.parcels 
    where st_dwithin(st_centroid(wkb_geometry), st_transform(st_setsrid(st_makepoint(lng, lat), 4326), 2898), radius)
    order by distance asc
$$ language sql stable;
