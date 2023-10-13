#custom library containing various SQL queries as callible functions

def get_FolioTitlesQuery():
    titles = """
    select 
        material_type__t.name as Material_Type,
        instance__t.title,
        item__t.volume,
        item__t.enumeration,
        item__t.chronology,
        holdings_record__t.id as holdingID,
        instance__t.id as instanceID
    from
        inventory.item__t
        join inventory.holdings_record__t on holdings_record__t.id = item__t.holdings_record_id
        join inventory.instance__t on instance__t.id::varchar(255) = holdings_record__t.instance_id
        join inventory.material_type__t on material_type__t.id = item__t.material_type_id
    Where
        holdings_record__t.effective_location_id in (
        select
        distinct(location__t.id)
        from
        inventory.location__t
        inner join inventory."loc-campus__t" on "loc-campus__t".id = location__t.campus_id
        where
        "loc-campus__t".code = 'RP'
        or
        "loc-campus__t".code = 'UM'
        )
        and( item__t.material_type_id in (
            select
                material_type__t.id
            from
                inventory.material_type__t)
        );  
    """
    return titles

def get_materialTypeQuery():
    materials = """
    select *
    from inventory.material_type__t
    """
    return materials

