-- DDL commands to bootstrap the DomiNode DB

-- Create group roles
-- Create staging schemas and assign access permissions
-- Tweak public schema's permissions
-- Create styles table
-- Install helper functions

-- Intended workflows
--
-- 1. Ingest data into the DB
--
-- 1.1. User copies data (by loading tables with QGIS database manager or using
--      other means) over to its department's staging schema
--
--      It is crucial that the dataset is named according to the DomiNode
--      naming conventions. Either set the table's name straight on import or
--      be sure to rename it as the first thing after import
--
--      naming example: ppd_schools_v0.1.0
--
-- 1.2. Once the table is in the department's schema and has the proper name,
--      call the `setStagingPermissions()` function with the table's fully
--      qualified name (i.e. including the schema) in order to set appropriate
--      access permissions to the other department members
--
--      example: SELECT setStagingPermissions('ppd_staging."ppd_schools_v0.1.0"';
--
--      After calling this function, all department users shall be able to
--      access and edit the table

-- 1.3. If this table should be visible to other department's users it may be
--      moved to the common staging schema, named `dominode_staging`. Call
--      the `moveToDominodeStagingSchema()` function with the table's fully
--      qualified name as argument
--
--      example: SELECT moveToDominodeStagingSchema('ppd_staging."ppd_schools_v0.1.0"');
--
--      After calling this function, the table will now be on the
--      `dominode_staging` schema. All users of the department that owns the
--      table still maintain the same access permissions and users from other
--      departments are able to use the table as read-only
--
-- 1.4. If the table needs to be renamed (for example, to change the version)
--      you may issue the usual SQL command and everything continues to work
--
--      examples: ALTER TABLE ppd_staging."ppd_schools_v0.1.0" RENAME TO "ppd_schools_v0.2.0";
--
-- 1.5. After all data edits are done, a user with the editor role may move the
--      table to the public schema, where it becomes read-only (i.e. no user is
--      able to modify it, with the exception of deleting it, which only the
--      department's editors can do). In order to do so, call the
--      moveTableToPublicSchema() function with the table's fully qualified name
--
--      example: SELECT moveTableToPublicSchema('ppd_staging."ppd_schools_v0.1.0"');
--
--
-- 2. Modify already published data
--
-- 2.1. Tables that go into the public schema become readonly. The intended
--      workflow for public data modifications is to copy the table to the
--      department's staging schema, modify the data and then publish a new
--      version of the table. Afterwards, the original version may be
--      appropriately managed (archived, deleted, etc). In order to copy the
--      table to the department's staging schema call the
--      `copyTableBackToStagingSchema()` function. You need to pass it as
--      arguments:
--
--      -  Fully qualified name of the table,
--      -  New fully qualified name
--
--      example: SELECT copyTableBackToStagingSchema('public."ppd_schools_v0.1.0"', 'ppd_staging."ppd_schools-.v0.2.0-dev"');

-- -----
-- ROLES
-- -----

CREATE ROLE admin WITH CREATEDB CREATEROLE;
CREATE ROLE replicator WITH REPLICATION;

CREATE ROLE dominode_user;
CREATE ROLE editor IN ROLE dominode_user;

CREATE ROLE ppd_user IN ROLE dominode_user;
CREATE ROLE ppd_editor IN ROLE ppd_user, editor;

CREATE ROLE lsd_user IN ROLE dominode_user;
CREATE ROLE lsd_editor IN ROLE lsd_user, editor;

-- ---------------
-- STAGING SCHEMAS
-- ---------------

CREATE SCHEMA IF NOT EXISTS dominode_staging AUTHORIZATION editor;
CREATE SCHEMA IF NOT EXISTS ppd_staging AUTHORIZATION ppd_editor;
CREATE SCHEMA IF NOT EXISTS lsd_staging AUTHORIZATION lsd_editor;

-- Grant schema access to the relevant roles
GRANT USAGE, CREATE ON SCHEMA dominode_staging TO dominode_user;
GRANT USAGE, CREATE ON SCHEMA ppd_staging TO ppd_user;
GRANT USAGE, CREATE ON SCHEMA lsd_staging TO lsd_user;

-- -------------
-- PUBLIC SCHEMA
-- -------------

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

CREATE OR REPLACE FUNCTION setStagingPermissions(qualifiedTableName varchar) RETURNS VOID AS $functionBody$
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


CREATE OR REPLACE FUNCTION moveTableToDominodeStagingSchema(qualifiedTableName varchar) RETURNS VOID AS $functionBody$
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
    PERFORM setStagingPermissions(qualifiedTableName);
    EXECUTE format('ALTER TABLE %s SET SCHEMA dominode_staging', qualifiedTableName);
    EXECUTE format('GRANT SELECT ON %s TO dominode_user', newQualifiedName);

END
$functionBody$
    LANGUAGE  plpgsql;


CREATE OR REPLACE FUNCTION moveTableToPublicSchema(qualifiedTableName varchar) RETURNS VOID AS $functionBody$
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


CREATE OR REPLACE FUNCTION copyTableBackToStagingSchema(qualifiedTableName varchar, newTableQualifiedName varchar) RETURNS VOID AS $functionBody$
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


-- Disable creation of objects on the public schema by default
REVOKE CREATE ON SCHEMA public FROM public;

-- Grant permission to editors for creating new objects on the public schema
GRANT CREATE ON SCHEMA public TO editor;


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