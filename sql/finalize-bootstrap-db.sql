-- DDL commands to bootstrap the DomiNode DB

-- Create the `layer_styles` table which is used by QGIS to save styles
CREATE TABLE IF NOT EXISTS public.layer_styles
(
    id                serial not null
        constraint layer_styles_pkey
            primary key,
    f_table_catalog   varchar,
    f_table_schema    varchar,
    f_table_name      varchar,
    f_geometry_column varchar,
    stylename         text,
    styleqml          xml,
    stylesld          xml,
    useasdefault      boolean,
    description       text,
    owner             varchar(63) default CURRENT_USER,
    ui                xml,
    update_time       timestamp   default CURRENT_TIMESTAMP
);

ALTER TABLE public.layer_styles OWNER TO editor;
GRANT SELECT ON public.layer_styles TO dominode_user;


-- Create helper functions in order to facilitate loading datasets

CREATE OR REPLACE FUNCTION DomiNodeSetStagingPermissions(qualifiedTableName varchar) RETURNS VOID AS $functionBody$
--   1. assign ownership to the group role
--
--      ALTER TABLE ppd_staging."ppd_rrmap_v0.0.1-staging" OWNER TO ppd_editor;
--
--   2. Grant relevant permissions to users
--
--      GRANT SELECT ON ppd_staging."ppd_rrmap_v0.0.1-staging" TO ppd_user;
    DECLARE
        unqualifiedName varchar;
        schemaName varchar;
        schemaDepartment varchar;
        userRoleName varchar;
    BEGIN
        schemaName := split_part(qualifiedTableName, '.', 1);
        unqualifiedName := replace(qualifiedTableName, concat(schemaName, '.'), '');
        unqualifiedName := replace(unqualifiedName, '"', '');
        schemaDepartment := split_part(unqualifiedName, '_', 1);
        userRoleName := concat(schemaDepartment, '_user');
        EXECUTE format('ALTER TABLE %s OWNER TO %I', qualifiedTableName, userRoleName);
        IF schemaName = 'dominode_staging' THEN
            EXECUTE format('GRANT SELECT ON %s TO dominode_user', qualifiedTableName);
        END IF;
    END

    $functionBody$
    LANGUAGE  plpgsql;


CREATE OR REPLACE FUNCTION DomiNodeMoveTableToDominodeStagingSchema(qualifiedTableName varchar) RETURNS VOID AS $functionBody$
    -- Move a table from a department's internal schema to the project-wide internal staging schema
    --
    -- Tables in the department's staging schema are only readable by department members, while those
    -- on the project-wide staging schema are readable by all users (but they are only editable by
    -- department members).
    --

DECLARE
    schemaName varchar;
    unqualifiedName varchar;
    newQualifiedName varchar;
BEGIN
    schemaName := split_part(qualifiedTableName, '.', 1);
    unqualifiedName := replace(qualifiedTableName, concat(schemaName, '.'), '');
    newQualifiedName := concat('dominode_staging.', format('%s', unqualifiedName));
    PERFORM DomiNodeSetStagingPermissions(qualifiedTableName);
    EXECUTE format('ALTER TABLE %s SET SCHEMA dominode_staging', qualifiedTableName);
    EXECUTE format('GRANT SELECT ON %s TO dominode_user', newQualifiedName);

END
$functionBody$
    LANGUAGE  plpgsql;


CREATE OR REPLACE FUNCTION DomiNodeMoveTableToPublicSchema(qualifiedTableName varchar) RETURNS VOID AS $functionBody$
    -- Move a table from a department's internal schema to the public schema
    --
    -- Moved table is renamed and assigned proper permissions.
    --
    -- Example usage:
    --
    -- SELECT moveToPublicSchema('ppd_staging."ppd_schools_v0.0.1"')
    --
    --

    DECLARE
        schemaName varchar;
        unqualifiedName varchar;
        publicQualifiedName varchar;
        ownerRole varchar;
    BEGIN
        schemaName := split_part(qualifiedTableName, '.', 1);
        unqualifiedName := replace(qualifiedTableName, concat(schemaName, '.'), '');
        unqualifiedName := replace(unqualifiedName, '"', '');
        publicQualifiedName := concat('public.', format('%I', unqualifiedName));
        EXECUTE format('SELECT tableowner FROM pg_tables where schemaname=%L AND tablename=%L', schemaName, unqualifiedName) INTO ownerRole;
        EXECUTE format('ALTER TABLE %s SET SCHEMA public', qualifiedTableName);
        EXECUTE format('GRANT SELECT ON %s TO public', publicQualifiedName);
        EXECUTE format('REVOKE INSERT, UPDATE, DELETE ON %s FROM %I', publicQualifiedName, ownerRole);

    END
    $functionBody$
    LANGUAGE  plpgsql;


CREATE OR REPLACE FUNCTION DomiNodeCopyTableBackToStagingSchema(qualifiedTableName varchar, newTableQualifiedName varchar) RETURNS VOID AS $functionBody$
    -- Make a copy of the input table into the department's staging schema
    --
    -- This function shall be used whenever a table needs to be edited
    --
    -- Any department user should be able to copy a table back to its own
    -- staging schema, regardless if the department owns the dataset or not.

DECLARE
    ownerRole varchar;
BEGIN
    ownerRole := concat(
        split_part(
            split_part(newTableQualifiedName, '.', 1),
            '_',
            1
        ),
        '_user'
    );
    EXECUTE format('CREATE TABLE %s (LIKE %s INCLUDING ALL)', newTableQualifiedName, qualifiedTableName);
    EXECUTE format('INSERT INTO %s SELECT * FROM %s', newTableQualifiedName, qualifiedTableName);
    EXECUTE format('ALTER TABLE %s OWNER TO %I', newTableQualifiedName, ownerRole);
END
$functionBody$
    LANGUAGE  plpgsql;


-- After the initial setup is done, perform the following:

-- 1. Create initial users
-- PPD users
-- CREATE USER ppd_editor1 PASSWORD 'ppd_editor1' IN ROLE ppd_editor, admin;
-- CREATE USER ppd_editor2 PASSWORD 'ppd_editor2' IN ROLE ppd_editor;
-- CREATE USER ppd_user1 PASSWORD 'ppd_user1' IN ROLE ppd_user;
-- LSD users
-- CREATE USER lsd_editor1 PASSWORD 'lsd_editor1' IN ROLE lsd_editor;
-- CREATE USER lsd_editor2 PASSWORD 'lsd_editor2' IN ROLE lsd_editor;
-- CREATE USER lsd_user1 PASSWORD 'lsd_user1' IN ROLE lsd_user;