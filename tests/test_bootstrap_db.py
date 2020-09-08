import sqlalchemy as sla
from sqlalchemy import exc

import pytest


@pytest.mark.parametrize('username, schemaname', [
    pytest.param('ppd_user1', 'ppd_staging', id='t1'),
    pytest.param('ppd_editor1', 'ppd_staging', id='t2'),
    pytest.param('ppd_user1', 'dominode_staging', id='t37'),
    pytest.param('ppd_editor1', 'dominode_staging', id='t38'),
])
def test_user_can_create_tables_on_staging_schema(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        username,
        schemaname
):
    engine = _connect_to_db(username, db_admin_credentials, db_users_credentials)
    user_department = username.partition('_')[0]
    table_name = f'{schemaname}."{user_department}_roads_v0.0.1"'
    with engine.connect() as connection:
        with connection.begin() as transaction:
            connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(
                    f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
            transaction.rollback()


@pytest.mark.parametrize('username, schemaname', [
    pytest.param('ppd_editor1', 'public', id='t68'),
])
def test_editor_user_can_create_tables_on_public(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        username,
        schemaname
):
    engine = _connect_to_db(username, db_admin_credentials, db_users_credentials)
    user_department = username.partition('_')[0]
    table_name = f'{schemaname}."{user_department}_roads_v0.0.1"'
    with engine.connect() as connection:
        with connection.begin() as transaction:
            connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            transaction.rollback()


@pytest.mark.parametrize('username, schemaname', [
    pytest.param('ppd_user1', 'public', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t67'),
])
def test_regular_user_cannot_create_tables_on_public(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        username,
        schemaname
):
    engine = _connect_to_db(username, db_admin_credentials, db_users_credentials)
    user_department = username.partition('_')[0]
    table_name = f'{schemaname}."{user_department}_roads_v0.0.1"'
    with engine.connect() as connection:
        with connection.begin() as transaction:
            connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            transaction.rollback()


@pytest.mark.parametrize('username, schemaname, expected_owner', [
    pytest.param('ppd_user1', 'ppd_staging', 'ppd_user', id='t13.1'),
    pytest.param('ppd_editor1', 'ppd_staging', 'ppd_user', id='t14.1'),
    pytest.param('ppd_user1', 'dominode_staging', 'ppd_user', id='t49.1'),
    pytest.param('ppd_editor1', 'dominode_staging', 'ppd_user', id='t50.1'),
])
def test_user_can_call_setStagingPermissions_on_staging_schema(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        username,
        schemaname,
        expected_owner
):
    engine = _connect_to_db(username, db_admin_credentials, db_users_credentials)
    user_department = username.partition('_')[0]
    unqualified_table_name = f'{user_department}_roads_v0.01'
    table_name = f'{schemaname}."{unqualified_table_name}"'
    with engine.connect() as connection:
        with connection.begin() as transaction:
            connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(
                    f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
            owner_result = connection.execute(
                sla.text(
                    f'SELECT tableowner from pg_tables WHERE schemaname = :schema AND tablename = :table'
                ),
                schema=schemaname,
                table=unqualified_table_name
            )
            owner = owner_result.scalar()
            assert owner == expected_owner
            transaction.rollback()


@pytest.mark.parametrize('username,schemaname', [
    pytest.param('ppd_user1', 'ppd_staging', id='t7.1'),
    pytest.param('ppd_editor1', 'ppd_staging', id='t8.1'),
    pytest.param('ppd_user1', 'dominode_staging', id='t43.1'),
    pytest.param('ppd_editor1', 'dominode_staging', id='t44.1'),
])
def test_user_can_insert_features_on_staging_schema(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        username,
        schemaname
):
    engine = _connect_to_db(username, db_admin_credentials, db_users_credentials)
    user_department = username.partition('_')[0]
    table_name = f'{schemaname}."{user_department}_roads_v0.0.1"'
    with engine.connect() as connection:
        with connection.begin() as transaction:
            connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(
                    f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
            insert_query = sla.text(
                f'INSERT INTO {table_name} (road_name, geom) VALUES (:name, ST_GeomFromText(:geom, 4326))'
            )
            insert_result = connection.execute(
                insert_query,
                name='dummy',
                geom='LINESTRING(-71.160 42.258, -71.160 42.259, -71.161 42.25)'
            )
            transaction.rollback()


@pytest.mark.parametrize('username, schemaname', [
    pytest.param('ppd_user1', 'ppd_staging', id='t5.1'),
    pytest.param('ppd_editor1', 'ppd_staging', id='t6.1'),
    pytest.param('ppd_user1', 'dominode_staging', id='t41.1'),
    pytest.param('ppd_editor1', 'dominode_staging', id='t42.1'),
])
def test_user_can_select_features_on_staging_schema(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        username,
        schemaname
):
    engine = _connect_to_db(username, db_admin_credentials, db_users_credentials)
    user_department = username.partition('_')[0]
    table_name = f'{schemaname}."{user_department}_roads_v0.0.1"'
    with engine.connect() as connection:
        with connection.begin() as transaction:
            connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(
                    f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
            connection.execute(
                sla.text(
                    f'INSERT INTO {table_name} (road_name, geom) '
                    f'VALUES (:name, ST_GeomFromText(:geom, 4326))'
                ),
                name='dummy',
                geom='LINESTRING(-71.160 42.258, -71.160 42.259, -71.161 42.25)'
            )
            select_result = connection.execute(
                sla.text(f'SELECT * FROM {table_name}'),
                name='dummy',
                geom='LINESTRING(-71.160 42.258, -71.160 42.259, -71.161 42.25)'
            )
            assert len(select_result.fetchall()) > 0
            transaction.rollback()


@pytest.mark.parametrize('username, schemaname', [
    pytest.param('ppd_user1', 'ppd_staging', id='t11.1'),
    pytest.param('ppd_editor1', 'ppd_staging', id='t12.1'),
    pytest.param('ppd_user1', 'dominode_staging', id='t47.1'),
    pytest.param('ppd_editor1', 'dominode_staging', id='t48.1'),
])
def test_user_can_delete_features_on_staging_schema(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        username,
        schemaname
):
    engine = _connect_to_db(username, db_admin_credentials, db_users_credentials)
    user_department = username.partition('_')[0]
    table_name = f'{schemaname}."{user_department}_roads_v0.0.1"'
    feature_name = 'dummy'
    with engine.connect() as connection:
        with connection.begin() as transaction:
            connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(
                    f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
            insert_query = sla.text(
                f'INSERT INTO {table_name} (road_name, geom) VALUES (:name, ST_GeomFromText(:geom, 4326))'
            )
            connection.execute(
                insert_query,
                name=feature_name,
                geom='LINESTRING(-71.160 42.258, -71.160 42.259, -71.161 42.25)'
            )
            delete_query = sla.text(
                f'DELETE FROM {table_name} WHERE road_name = :name'
            )
            connection.execute(
                delete_query,
                name=feature_name
            )
            transaction.rollback()


@pytest.mark.parametrize('username,schemaname', [
    pytest.param('ppd_user1', 'ppd_staging', id='t9.1'),
    pytest.param('ppd_editor1', 'ppd_staging', id='t10.1'),
    pytest.param('ppd_user1', 'dominode_staging', id='t45.1'),
    pytest.param('ppd_editor1', 'dominode_staging', id='t46.1'),
])
def test_user_can_update_features_on_staging_schema(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        username,
        schemaname
):
    engine = _connect_to_db(username, db_admin_credentials, db_users_credentials)
    user_department = username.partition('_')[0]
    table_name = f'{schemaname}."{user_department}_roads_v0.0.1"'
    with engine.connect() as connection:
        with connection.begin() as transaction:
            connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
            insert_query = sla.text(
                f'INSERT INTO {table_name} (road_name, geom) VALUES (:name, ST_GeomFromText(:geom, 4326))'
            )
            original_name = 'dummy'
            connection.execute(
                insert_query,
                name=original_name,
                geom='LINESTRING(-71.160 42.258, -71.160 42.259, -71.161 42.25)'
            )
            update_query = sla.text(
                f'UPDATE {table_name} SET road_name = :new_name WHERE road_name = :original_name'
            )
            connection.execute(
                update_query,
                new_name='new_dummy',
                original_name=original_name
            )
            transaction.rollback()


@pytest.mark.parametrize('username,schemaname', [
    pytest.param('ppd_user1', 'ppd_staging', id='t3.1'),
    pytest.param('ppd_editor1', 'ppd_staging', id='t4.1'),
    pytest.param('ppd_user1', 'dominode_staging', id='t39.1'),
    pytest.param('ppd_editor1', 'dominode_staging', id='t40.1'),
])
def test_user_can_delete_table_on_staging_schema(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        username,
        schemaname
):
    engine = _connect_to_db(username, db_admin_credentials, db_users_credentials)
    user_department = username.partition('_')[0]
    table_name = f'{schemaname}."{user_department}_roads_v0.0.1"'
    with engine.connect() as connection:
        with connection.begin() as transaction:
            create_result = connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
            connection.execute(f'DROP TABLE {table_name}')
            transaction.rollback()


@pytest.mark.parametrize('creator_username, modifier_username, schemaname', [
    pytest.param('ppd_user1', 'ppd_user2', 'ppd_staging', id='t5'),
    pytest.param('ppd_user1', 'ppd_user2', 'dominode_staging', id='t41'),
    pytest.param('ppd_user1', 'ppd_editor1', 'ppd_staging', id='t6'),
    pytest.param('ppd_user1', 'ppd_editor1', 'dominode_staging', id='t42'),
    pytest.param('ppd_user1', 'lsd_user1', 'dominode_staging', id='t55'),
    pytest.param('ppd_user1', 'lsd_editor1', 'dominode_staging', id='t56'),
])
def test_user_can_select_features_from_any_table_staging_schema(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        creator_username,
        modifier_username,
        schemaname,

):
    creator_engine = _connect_to_db(creator_username, db_admin_credentials, db_users_credentials)
    creator_department = creator_username.partition('_')[0]
    table_name = f'{schemaname}."{creator_department}_roads_v0.0.1"'
    with creator_engine.connect() as connection:
        with connection.begin() as transaction:
            create_result = connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
            insert_query = sla.text(
                f'INSERT INTO {table_name} (road_name, geom) VALUES (:name, ST_GeomFromText(:geom, 4326))'
            )
            insert_result = connection.execute(
                insert_query,
                name='dummy',
                geom='LINESTRING(-71.160 42.258, -71.160 42.259, -71.161 42.25)'
            )
    modifier_engine = _connect_to_db(modifier_username, db_admin_credentials, db_users_credentials)
    with modifier_engine.connect() as connection:
        with connection.begin() as transaction:
            select_result = connection.execute(
                sla.text(f'SELECT * FROM {table_name}'))
            assert len(select_result.fetchall()) > 0
    # clean up the DB
    with creator_engine.connect() as connection:
        connection.execute(f'DROP table {table_name}')


@pytest.mark.parametrize('creator_username, modifier_username, schemaname', [
    pytest.param('ppd_user1', 'ppd_user2', 'ppd_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t81'),
    pytest.param('ppd_user1', 'ppd_editor1', 'ppd_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t82'),
    pytest.param('ppd_user1', 'ppd_user2', 'dominode_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t83'),
    pytest.param('ppd_user1', 'ppd_editor1', 'dominode_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t84'),
    pytest.param('ppd_user1', 'lsd_user1', 'ppd_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t31'),
    pytest.param('ppd_user1', 'lsd_editor1', 'ppd_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t32'),
    pytest.param('ppd_user1', 'lsd_user1', 'dominode_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t63'),
    pytest.param('ppd_user1', 'lsd_editor1', 'dominode_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t64'),

])
def test_user_cannot_call_setStagingPermissions_on_table_created_by_another_user_on_staging_schema(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        creator_username,
        modifier_username,
        schemaname,

):
    creator_engine = _connect_to_db(creator_username, db_admin_credentials, db_users_credentials)
    creator_department = creator_username.partition('_')[0]
    table_name = f'{schemaname}."{creator_department}_roads_v0.0.1"'
    with creator_engine.connect() as connection:
        with connection.begin() as transaction:
            create_result = connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
    modifier_engine = _connect_to_db(modifier_username, db_admin_credentials, db_users_credentials)
    with modifier_engine.connect() as connection:
        transaction = connection.begin()
        try:
            connection.execute(
                sla.text(f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
        finally:
            with creator_engine.connect() as connection:
                connection.execute(f'DROP table {table_name}')


@pytest.mark.parametrize('creator_username, modifier_username, schemaname', [
    pytest.param('ppd_user1', 'ppd_user2', 'ppd_staging', id='t15.5'),
    pytest.param('ppd_user1', 'ppd_editor1', 'ppd_staging', id='t16.5'),

])
def test_same_department_user_can_call_moveTableToDominodeStaging_on_table_created_by_other_user(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        creator_username,
        modifier_username,
        schemaname,

):
    creator_engine = _connect_to_db(creator_username, db_admin_credentials, db_users_credentials)
    creator_department = creator_username.partition('_')[0]
    table_name = f'{schemaname}."{creator_department}_roads_v0.0.1"'
    with creator_engine.connect() as connection:
        with connection.begin() as transaction:
            create_result = connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
    modifier_engine = _connect_to_db(modifier_username, db_admin_credentials, db_users_credentials)
    with modifier_engine.connect() as connection:
        with connection.begin() as transaction:
            connection.execute(
                sla.text(f'SELECT DomiNodeMoveTableToDominodeStagingSchema(\'{table_name}\')')
            )
    new_table_name = table_name.replace(schemaname, 'dominode_staging')
    with creator_engine.connect() as connection:
        connection.execute(f'DROP table {new_table_name}')


@pytest.mark.parametrize('creator_username, modifier_username, schemaname', [
    pytest.param('ppd_user1', 'lsd_user2', 'ppd_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t33'),
    pytest.param('ppd_user1', 'lsd_editor1', 'ppd_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t34'),

])
def test_user_cannot_call_moveTableToDominodeStagingSchema_on_table_owned_by_another_department(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        creator_username,
        modifier_username,
        schemaname,

):
    creator_engine = _connect_to_db(creator_username, db_admin_credentials, db_users_credentials)
    creator_department = creator_username.partition('_')[0]
    table_name = f'{schemaname}."{creator_department}_roads_v0.0.1"'
    with creator_engine.connect() as connection:
        with connection.begin() as transaction:
            create_result = connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
    modifier_engine = _connect_to_db(modifier_username, db_admin_credentials, db_users_credentials)
    with modifier_engine.connect() as connection:
        transaction = connection.begin()
        try:
            connection.execute(
                sla.text(f'SELECT DomiNodeMoveTableToDominodeStagingSchema(\'{table_name}\')')
            )
        finally:
            with creator_engine.connect() as connection:
                connection.execute(f'DROP table {table_name}')


@pytest.mark.parametrize('creator_username, modifier_username, schemaname', [
    pytest.param('ppd_user1', 'lsd_user2', 'ppd_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t35'),
    pytest.param('ppd_user1', 'lsd_editor1', 'ppd_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t36'),
    pytest.param('ppd_user1', 'lsd_user2', 'dominode_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t65'),
    pytest.param('ppd_user1', 'lsd_editor1', 'dominode_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t66'),
])
def test_user_cannot_call_moveTableToPublicSchema_on_table_owned_by_another_department(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        creator_username,
        modifier_username,
        schemaname,

):
    creator_engine = _connect_to_db(creator_username, db_admin_credentials, db_users_credentials)
    creator_department = creator_username.partition('_')[0]
    table_name = f'{schemaname}."{creator_department}_roads_v0.0.1"'
    with creator_engine.connect() as connection:
        with connection.begin() as transaction:
            create_result = connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
    modifier_engine = _connect_to_db(modifier_username, db_admin_credentials, db_users_credentials)
    with modifier_engine.connect() as connection:
        transaction = connection.begin()
        try:
            connection.execute(
                sla.text(f'SELECT DomiNodeMoveTableToPublicSchema(\'{table_name}\')')
            )
        finally:
            with creator_engine.connect() as connection:
                connection.execute(f'DROP table {table_name}')


@pytest.mark.parametrize('creator_username, modifier_username, schemaname', [
    pytest.param('ppd_user1', 'ppd_user2', 'ppd_staging', id='t7.2'),
    pytest.param('ppd_user1', 'ppd_user2', 'dominode_staging', id='t43.2'),
    pytest.param('ppd_user1', 'ppd_editor1', 'ppd_staging', id='t8.2'),
    pytest.param('ppd_user1', 'ppd_editor1', 'dominode_staging', id='t44.2'),
])
def test_same_department_user_can_insert_features_on_table_created_by_another_user_on_staging_schema(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        creator_username,
        modifier_username,
        schemaname,

):
    creator_engine = _connect_to_db(creator_username, db_admin_credentials, db_users_credentials)
    creator_department = creator_username.partition('_')[0]
    table_name = f'{schemaname}."{creator_department}_roads_v0.0.1"'
    with creator_engine.connect() as connection:
        with connection.begin() as transaction:
            create_result = connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
    modifier_engine = _connect_to_db(modifier_username, db_admin_credentials, db_users_credentials)
    with modifier_engine.connect() as connection:
        with connection.begin() as transaction:
            insert_query = sla.text(
                f'INSERT INTO {table_name} (road_name, geom) VALUES (:name, ST_GeomFromText(:geom, 4326))'
            )
            insert_result = connection.execute(
                insert_query,
                name='dummy',
                geom='LINESTRING(-71.160 42.258, -71.160 42.259, -71.161 42.25)'
            )
    # clean up the DB
    with creator_engine.connect() as connection:
        connection.execute(f'DROP table {table_name}')


@pytest.mark.parametrize('creator_username, modifier_username, schemaname', [
    pytest.param('ppd_user1', 'ppd_user2', 'ppd_staging', id='t11.2'),
    pytest.param('ppd_user1', 'ppd_user2', 'dominode_staging', id='t47.2'),
    pytest.param('ppd_user1', 'ppd_editor1', 'ppd_staging', id='t12.2'),
    pytest.param('ppd_user1', 'ppd_editor1', 'dominode_staging', id='t48.2'),
])
def test_same_department_user_can_delete_features_on_table_created_by_another_user_on_staging_schema(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        creator_username,
        modifier_username,
        schemaname,

):
    creator_engine = _connect_to_db(creator_username, db_admin_credentials, db_users_credentials)
    creator_department = creator_username.partition('_')[0]
    table_name = f'{schemaname}."{creator_department}_roads_v0.0.1"'
    feature_name = 'dummy'
    with creator_engine.connect() as connection:
        with connection.begin() as transaction:
            create_result = connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
            insert_query = sla.text(
                f'INSERT INTO {table_name} (road_name, geom) VALUES (:name, ST_GeomFromText(:geom, 4326))'
            )
            insert_result = connection.execute(
                insert_query,
                name=feature_name,
                geom='LINESTRING(-71.160 42.258, -71.160 42.259, -71.161 42.25)'
            )
    modifier_engine = _connect_to_db(modifier_username, db_admin_credentials, db_users_credentials)
    with modifier_engine.connect() as connection:
        with connection.begin() as transaction:
            delete_query = sla.text(
                f'DELETE FROM {table_name} WHERE road_name = :name'
            )
            delete_result = connection.execute(
                delete_query,
                name=feature_name,
            )
    # clean up the DB
    with creator_engine.connect() as connection:
        connection.execute(f'DROP table {table_name}')


@pytest.mark.parametrize('creator_username, modifier_username, schemaname', [
    pytest.param('ppd_user1', 'ppd_user2', 'ppd_staging', id='t9.2'),
    pytest.param('ppd_user1', 'ppd_user2', 'dominode_staging', id='t45.2'),
    pytest.param('ppd_user1', 'ppd_editor1', 'ppd_staging', id='t10.2'),
    pytest.param('ppd_user1', 'ppd_editor1', 'dominode_staging', id='t46.2'),
])
def test_same_department_user_can_update_features_on_table_created_by_another_user_on_staging_schema(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        creator_username,
        modifier_username,
        schemaname,
):
    creator_engine = _connect_to_db(creator_username, db_admin_credentials, db_users_credentials)
    creator_department = creator_username.partition('_')[0]
    table_name = f'{schemaname}."{creator_department}_roads_v0.0.1"'
    with creator_engine.connect() as connection:
        with connection.begin() as transaction:
            create_result = connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
            insert_query = sla.text(
                f'INSERT INTO {table_name} (road_name, geom) VALUES (:name, ST_GeomFromText(:geom, 4326))'
            )
            original_name = 'dummy'
            insert_result = connection.execute(
                insert_query,
                name=original_name,
                geom='LINESTRING(-71.160 42.258, -71.160 42.259, -71.161 42.25)'
            )
    modifier_engine = _connect_to_db(modifier_username, db_admin_credentials, db_users_credentials)
    with modifier_engine.connect() as connection:
        with connection.begin() as transaction:
            update_query = sla.text(
                f'UPDATE {table_name} SET road_name = :new_name WHERE road_name = :original_name'
            )
            update_result = connection.execute(
                update_query,
                new_name='new_dummy',
                original_name=original_name
            )
    # clean up the DB
    with creator_engine.connect() as connection:
        connection.execute(f'DROP table {table_name}')


@pytest.mark.parametrize('creator_username, modifier_username, schemaname', [
    pytest.param('ppd_user1', 'ppd_user2', 'ppd_staging', id='t3.2'),
    pytest.param('ppd_user1', 'ppd_user2', 'dominode_staging', id='t39.2'),
    pytest.param('ppd_user1', 'ppd_editor1', 'ppd_staging', id='t4.2'),
    pytest.param('ppd_user1', 'ppd_editor1', 'dominode_staging', id='t40.2'),
])
def test_same_department_user_can_delete_table_created_by_another_user_on_staging_schema(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        creator_username,
        modifier_username,
        schemaname
):
    creator_engine = _connect_to_db(creator_username, db_admin_credentials, db_users_credentials)
    creator_department = creator_username.partition('_')[0]
    table_name = f'{schemaname}."{creator_department}_roads_v0.0.1"'
    with creator_engine.connect() as connection:
        with connection.begin() as transaction:
            create_result = connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
    modifier_engine = _connect_to_db(modifier_username, db_admin_credentials, db_users_credentials)
    with modifier_engine.connect() as connection:
        connection.execute(f'DROP table {table_name}')


@pytest.mark.parametrize('username, schemaname', [
    pytest.param('ppd_user1', 'lsd_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t19'),
    pytest.param('ppd_editor1', 'lsd_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t20'),
])
def test_user_cannot_create_tables_on_another_department_staging_schema(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        username,
        schemaname
):
    engine = _connect_to_db(username, db_admin_credentials, db_users_credentials)
    user_department = username.partition('_')[0]
    table_name = f'{schemaname}."{user_department}_roads_v0.0.1"'
    with engine.connect() as connection:
        with connection.begin() as transaction:
            create_result = connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
            transaction.rollback()


@pytest.mark.parametrize('creator_username, modifier_username, schemaname', [
    pytest.param('ppd_user1', 'lsd_user1', 'ppd_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t23'),
    pytest.param('ppd_user1', 'lsd_editor1', 'ppd_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t24'),
])
def test_user_cannot_select_features_on_table_from_another_department_staging_schema(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        creator_username,
        modifier_username,
        schemaname,

):
    creator_engine = _connect_to_db(creator_username, db_admin_credentials, db_users_credentials)
    creator_department = creator_username.partition('_')[0]
    table_name = f'{schemaname}."{creator_department}_roads_v0.0.1"'
    with creator_engine.connect() as connection:
        with connection.begin() as transaction:
            create_result = connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            insert_query = sla.text(
                f'INSERT INTO {table_name} (road_name, geom) VALUES (:name, ST_GeomFromText(:geom, 4326))'
            )
            insert_result = connection.execute(
                insert_query,
                name='dummy',
                geom='LINESTRING(-71.160 42.258, -71.160 42.259, -71.161 42.25)'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
    modifier_engine = _connect_to_db(modifier_username, db_admin_credentials, db_users_credentials)
    with modifier_engine.connect() as connection:
        # not using a context manager for managing the transaction here because
        # we need explicit control over the clean up action, since it shall
        # be performed by another DB user
        try:
            select_result = connection.execute(
                sla.text(f'SELECT * FROM {table_name}'))
        finally:  # clean up the DB
            with creator_engine.connect() as connection:
                connection.execute(f'DROP table {table_name}')


@pytest.mark.parametrize('creator_username, modifier_username, schemaname', [
    pytest.param('ppd_user1', 'lsd_user1', 'ppd_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t25'),
    pytest.param('ppd_user1', 'lsd_editor1', 'ppd_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t26'),
    pytest.param('ppd_user1', 'lsd_user1', 'dominode_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t57'),
    pytest.param('ppd_user1', 'lsd_editor1', 'dominode_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t58'),
])
def test_user_cannot_insert_features_on_table_owned_by_another_department(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        creator_username,
        modifier_username,
        schemaname,

):
    creator_engine = _connect_to_db(creator_username, db_admin_credentials, db_users_credentials)
    creator_department = creator_username.partition('_')[0]
    table_name = f'{schemaname}."{creator_department}_roads_v0.0.1"'
    with creator_engine.connect() as connection:
        with connection.begin() as transaction:
            create_result = connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
    modifier_engine = _connect_to_db(modifier_username, db_admin_credentials, db_users_credentials)
    with modifier_engine.connect() as connection:
        # not using a context manager for managing the transaction here because
        # we need explicit control over the clean up action, since it shall
        # be performed by another DB user
        transaction = connection.begin()
        try:
            insert_query = sla.text(
                f'INSERT INTO {table_name} (road_name, geom) VALUES (:name, ST_GeomFromText(:geom, 4326))'
            )
            insert_result = connection.execute(
                insert_query,
                name='dummy',
                geom='LINESTRING(-71.160 42.258, -71.160 42.259, -71.161 42.25)'
            )
        except exc.ProgrammingError:
            transaction.rollback()
            raise
        finally:  # clean up the DB
            with creator_engine.connect() as connection:
                connection.execute(f'DROP table {table_name}')


@pytest.mark.parametrize('creator_username, modifier_username, schemaname', [
    pytest.param('ppd_user1', 'lsd_user1', 'ppd_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t29'),
    pytest.param('ppd_user1', 'lsd_editor1', 'ppd_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t30'),
    pytest.param('ppd_user1', 'lsd_user1', 'dominode_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t61'),
    pytest.param('ppd_user1', 'lsd_editor1', 'dominode_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t62'),
])
def test_user_cannot_delete_features_on_table_owned_by_another_department(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        creator_username,
        modifier_username,
        schemaname,

):
    creator_engine = _connect_to_db(creator_username, db_admin_credentials, db_users_credentials)
    creator_department = creator_username.partition('_')[0]
    table_name = f'{schemaname}."{creator_department}_roads_v0.0.1"'
    feature_name = 'dummy'
    with creator_engine.connect() as connection:
        with connection.begin() as transaction:
            create_result = connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
            insert_query = sla.text(
                f'INSERT INTO {table_name} (road_name, geom) VALUES (:name, ST_GeomFromText(:geom, 4326))'
            )
            insert_result = connection.execute(
                insert_query,
                name=feature_name,
                geom='LINESTRING(-71.160 42.258, -71.160 42.259, -71.161 42.25)'
            )
    modifier_engine = _connect_to_db(modifier_username, db_admin_credentials, db_users_credentials)
    with modifier_engine.connect() as connection:
        # not using a context manager for managing the transaction here because
        # we need explicit control over the clean up action, since it shall
        # be performed by another DB user
        transaction = connection.begin()
        try:
            delete_result = connection.execute(
                sla.text(f'DELETE FROM {table_name} WHERE road_name = :name'),
                name=feature_name,
            )
        except exc.ProgrammingError:
            transaction.rollback()
            raise
        finally:  # clean up the DB
            with creator_engine.connect() as connection:
                connection.execute(f'DROP table {table_name}')


@pytest.mark.parametrize('creator_username, modifier_username, schemaname', [
    pytest.param('ppd_user1', 'lsd_user1', 'ppd_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t27'),
    pytest.param('ppd_user1', 'lsd_editor1', 'ppd_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t28'),
    pytest.param('ppd_user1', 'lsd_user1', 'dominode_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t59'),
    pytest.param('ppd_user1', 'lsd_editor1', 'dominode_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t60'),
])
def test_user_cannot_update_features_on_table_owned_by_another_department(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        creator_username,
        modifier_username,
        schemaname,
):
    creator_engine = _connect_to_db(creator_username, db_admin_credentials, db_users_credentials)
    creator_department = creator_username.partition('_')[0]
    table_name = f'{schemaname}."{creator_department}_roads_v0.0.1"'
    with creator_engine.connect() as connection:
        with connection.begin() as transaction:
            create_result = connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
            insert_query = sla.text(
                f'INSERT INTO {table_name} (road_name, geom) VALUES (:name, ST_GeomFromText(:geom, 4326))'
            )
            original_name = 'dummy'
            insert_result = connection.execute(
                insert_query,
                name=original_name,
                geom='LINESTRING(-71.160 42.258, -71.160 42.259, -71.161 42.25)'
            )
    modifier_engine = _connect_to_db(modifier_username, db_admin_credentials, db_users_credentials)
    with modifier_engine.connect() as connection:
        # not using a context manager for managing the transaction here because
        # we need explicit control over the clean up action, since it shall
        # be performed by another DB user
        transaction = connection.begin()
        try:
            update_query = sla.text(
                f'UPDATE {table_name} SET road_name = :new_name WHERE road_name = :original_name'
            )
            update_result = connection.execute(
                update_query,
                new_name='new_dummy',
                original_name=original_name
            )
        except exc.ProgrammingError:
            transaction.rollback()
            raise
        finally:  # clean up the DB
            with creator_engine.connect() as connection:
                connection.execute(f'DROP table {table_name}')


@pytest.mark.parametrize('creator_username, modifier_username, schemaname', [
    pytest.param('ppd_user1', 'lsd_user2', 'ppd_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t21'),
    pytest.param('ppd_user1', 'lsd_editor2', 'ppd_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t22'),
    pytest.param('ppd_user1', 'lsd_user2', 'dominode_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t53'),
    pytest.param('ppd_user1', 'lsd_editor2', 'dominode_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t54'),
])
def test_user_cannot_delete_table_owned_by_another_department(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        creator_username,
        modifier_username,
        schemaname
):
    creator_engine = _connect_to_db(creator_username, db_admin_credentials, db_users_credentials)
    creator_department = creator_username.partition('_')[0]
    table_name = f'{schemaname}."{creator_department}_roads_v0.0.1"'
    with creator_engine.connect() as connection:
        with connection.begin() as transaction:
            create_result = connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
    modifier_engine = _connect_to_db(modifier_username, db_admin_credentials, db_users_credentials)
    with modifier_engine.connect() as connection:
        try:
            connection.execute(f'DROP table {table_name}')
        finally:  # clean up the DB
            with creator_engine.connect() as connection:
                connection.execute(f'DROP table {table_name}')


@pytest.mark.parametrize('username, schemaname, expected_owner', [
    pytest.param('ppd_user1', 'ppd_staging', 'ppd_user', id='t15.1'),
    pytest.param('ppd_editor1', 'ppd_staging', 'ppd_user', id='t16.1'),
    pytest.param('lsd_user1', 'lsd_staging', 'lsd_user', id='t15.2'),
    pytest.param('lsd_editor1', 'lsd_staging', 'lsd_user', id='t16.2'),
])
def test_user_can_call_moveTableToDominodeStagingSchema(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        username,
        schemaname,
        expected_owner
):
    engine = _connect_to_db(username, db_admin_credentials, db_users_credentials)
    user_department = username.partition('_')[0]
    unqualified_table_name = f'{user_department}_roads_v0.01'
    table_name = f'{schemaname}."{unqualified_table_name}"'
    with engine.connect() as connection:
        with connection.begin() as transaction:
            create_result = connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeMoveTableToDominodeStagingSchema(\'{table_name}\')')
            )
            owner_result = connection.execute(
                sla.text(
                    f'SELECT tableowner from pg_tables WHERE schemaname = :schema AND tablename = :table'
                ),
                schema='dominode_staging',
                table=unqualified_table_name
            )
            owner = owner_result.scalar()
            assert owner == expected_owner
            transaction.rollback()


@pytest.mark.parametrize('username, schemaname, expected_owner', [
    pytest.param('ppd_user1', 'ppd_staging', 'ppd_user', id='t15.3'),
    pytest.param('lsd_user1', 'lsd_staging', 'lsd_user', id='t15.4'),
    pytest.param('ppd_editor1', 'ppd_staging', 'ppd_user', id='t16.3'),
    pytest.param('lsd_editor1', 'lsd_staging', 'lsd_user', id='t16.4'),
])
def test_user_can_call_moveTableToDominodeStagingSchema_without_calling_setStagingPermissions_first(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        username,
        schemaname,
        expected_owner
):
    engine = _connect_to_db(username, db_admin_credentials, db_users_credentials)
    user_department = username.partition('_')[0]
    unqualified_table_name = f'{user_department}_roads_v0.01'
    table_name = f'{schemaname}."{unqualified_table_name}"'
    with engine.connect() as connection:
        with connection.begin() as transaction:
            create_result = connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeMoveTableToDominodeStagingSchema(\'{table_name}\')')
            )
            owner_result = connection.execute(
                sla.text(
                    f'SELECT tableowner from pg_tables WHERE schemaname = :schema AND tablename = :table'
                ),
                schema='dominode_staging',
                table=unqualified_table_name
            )
            owner = owner_result.scalar()
            assert owner == expected_owner
            transaction.rollback()


@pytest.mark.parametrize('username, schemaname, expected_owner', [
    pytest.param('ppd_editor1', 'ppd_staging', 'ppd_user', id='t18.1'),
    pytest.param('ppd_editor1', 'dominode_staging', 'ppd_user', id='t52.1'),
    pytest.param('lsd_editor1', 'lsd_staging', 'lsd_user', id='t18.2'),
    pytest.param('lsd_editor1', 'dominode_staging', 'lsd_user', id='t52.2'),
])
def test_editor_user_can_call_moveTableToPublicSchema(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        username,
        schemaname,
        expected_owner
):
    engine = _connect_to_db(username, db_admin_credentials, db_users_credentials)
    user_department = username.partition('_')[0]
    unqualified_table_name = f'{user_department}_roads_v0.01'
    table_name = f'{schemaname}."{unqualified_table_name}"'
    with engine.connect() as connection:
        with connection.begin() as transaction:
            create_result = connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeMoveTableToDominodeStagingSchema(\'{table_name}\')')
            )
            owner_result = connection.execute(
                sla.text(
                    f'SELECT tableowner from pg_tables WHERE schemaname = :schema AND tablename = :table'
                ),
                schema='dominode_staging',
                table=unqualified_table_name
            )
            owner = owner_result.scalar()
            assert owner == expected_owner
            transaction.rollback()


@pytest.mark.parametrize('username, schemaname, expected_owner', [
    pytest.param('ppd_user1', 'ppd_staging', 'ppd_user', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t17.1'),
    pytest.param('ppd_user1', 'dominode_staging', 'ppd_user', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t51.1'),
    pytest.param('lsd_user1', 'lsd_staging', 'lsd_user', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t17.2'),
    pytest.param('lsd_user1', 'dominode_staging', 'lsd_user', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t51.2'),
])
def test_regular_user_cannot_call_moveTableToPublicSchema(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        username,
        schemaname,
        expected_owner
):
    engine = _connect_to_db(username, db_admin_credentials, db_users_credentials)
    user_department = username.partition('_')[0]
    unqualified_table_name = f'{user_department}_roads_v0.01'
    table_name = f'{schemaname}."{unqualified_table_name}"'
    with engine.connect() as connection:
        with connection.begin() as transaction:
            create_result = connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeMoveTableToPublicSchema(\'{table_name}\')')
            )
            owner_result = connection.execute(
                sla.text(
                    f'SELECT tableowner from pg_tables WHERE schemaname = :schema AND tablename = :table'
                ),
                schema='public',
                table=unqualified_table_name
            )
            owner = owner_result.scalar()
            assert owner == expected_owner
            transaction.rollback()


@pytest.mark.parametrize('username,schemaname', [
    pytest.param('ppd_editor1', 'ppd_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t74.1'),
    pytest.param('ppd_editor1', 'dominode_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t74.2'),
])
def test_editor_user_cannot_insert_features_on_public_schema(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        username,
        schemaname
):
    engine = _connect_to_db(username, db_admin_credentials, db_users_credentials)
    user_department = username.partition('_')[0]
    table_name = f'{schemaname}."{user_department}_roads_v0.0.1"'
    with engine.connect() as connection:
        with connection.begin() as transaction:
            create_result = connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeMoveTableToPublicSchema(\'{table_name}\')')
            )
            public_table_name = table_name.replace(schemaname, 'public')
            insert_query = sla.text(
                f'INSERT INTO {public_table_name} (road_name, geom) VALUES (:name, ST_GeomFromText(:geom, 4326))'
            )
            insert_result = connection.execute(
                insert_query,
                name='dummy',
                geom='LINESTRING(-71.160 42.258, -71.160 42.259, -71.161 42.25)'
            )
            transaction.rollback()


@pytest.mark.parametrize('username,schemaname', [
    pytest.param('ppd_editor1', 'ppd_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t76.1'),
    pytest.param('ppd_editor1', 'dominode_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t76.2'),
])
def test_editor_user_cannot_update_features_on_public_schema(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        username,
        schemaname
):
    engine = _connect_to_db(username, db_admin_credentials, db_users_credentials)
    user_department = username.partition('_')[0]
    table_name = f'{schemaname}."{user_department}_roads_v0.0.1"'
    with engine.connect() as connection:
        with connection.begin() as transaction:
            create_result = connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
            insert_query = sla.text(
                f'INSERT INTO {table_name} (road_name, geom) VALUES (:name, ST_GeomFromText(:geom, 4326))'
            )
            original_name = 'dummy'
            insert_result = connection.execute(
                insert_query,
                name=original_name,
                geom='LINESTRING(-71.160 42.258, -71.160 42.259, -71.161 42.25)'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeMoveTableToPublicSchema(\'{table_name}\')')
            )
            public_table_name = table_name.replace(schemaname, 'public')
            update_query = sla.text(
                f'UPDATE {public_table_name} SET road_name = :new_name WHERE road_name = :original_name'
            )
            update_result = connection.execute(
                update_query,
                new_name='new_dummy',
                original_name=original_name
            )
            transaction.rollback()


@pytest.mark.parametrize('username,schemaname', [
    pytest.param('ppd_editor1', 'ppd_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t78.1'),
    pytest.param('ppd_editor1', 'dominode_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t78.2'),
])
def test_editor_user_cannot_delete_features_on_public_schema(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        username,
        schemaname
):
    engine = _connect_to_db(username, db_admin_credentials, db_users_credentials)
    user_department = username.partition('_')[0]
    table_name = f'{schemaname}."{user_department}_roads_v0.0.1"'
    with engine.connect() as connection:
        with connection.begin() as transaction:
            create_result = connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
            insert_query = sla.text(
                f'INSERT INTO {table_name} (road_name, geom) VALUES (:name, ST_GeomFromText(:geom, 4326))'
            )
            original_name = 'dummy'
            insert_result = connection.execute(
                insert_query,
                name=original_name,
                geom='LINESTRING(-71.160 42.258, -71.160 42.259, -71.161 42.25)'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeMoveTableToPublicSchema(\'{table_name}\')')
            )
            public_table_name = table_name.replace(schemaname, 'public')
            update_query = sla.text(
                f'DELETE FROM {public_table_name} WHERE road_name = :original_name'
            )
            update_result = connection.execute(
                update_query,
                original_name=original_name
            )
            transaction.rollback()


@pytest.mark.parametrize('username,schemaname', [
    pytest.param('ppd_editor1', 'ppd_staging', id='t70.1'),
    pytest.param('ppd_editor1', 'dominode_staging', id='t70.2'),
])
def test_editor_user_can_delete_table_from_public_schema(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        username,
        schemaname
):
    engine = _connect_to_db(username, db_admin_credentials, db_users_credentials)
    user_department = username.partition('_')[0]
    table_name = f'{schemaname}."{user_department}_roads_v0.0.1"'
    with engine.connect() as connection:
        with connection.begin() as transaction:
            create_result = connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeMoveTableToPublicSchema(\'{table_name}\')')
            )
            public_table_name = table_name.replace(schemaname, 'public')
            connection.execute(f'DROP TABLE {public_table_name}')
            transaction.rollback()


@pytest.mark.parametrize('creator_username, modifier_username, schemaname', [
    pytest.param('ppd_editor1', 'ppd_user1', 'ppd_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t73.1'),
    pytest.param('ppd_editor1', 'ppd_user1', 'dominode_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t73.2'),
    pytest.param('ppd_editor1', 'lsd_user1', 'dominode_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t73.3'),
])
def test_regular_user_cannot_insert_features_on_table_in_public_schema(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        creator_username,
        modifier_username,
        schemaname,

):
    creator_engine = _connect_to_db(creator_username, db_admin_credentials, db_users_credentials)
    creator_department = creator_username.partition('_')[0]
    table_name = f'{schemaname}."{creator_department}_roads_v0.0.1"'
    with creator_engine.connect() as connection:
        with connection.begin() as transaction:
            create_result = connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeMoveTableToPublicSchema(\'{table_name}\')')
            )
    public_table_name = table_name.replace(schemaname, 'public')
    modifier_engine = _connect_to_db(modifier_username, db_admin_credentials, db_users_credentials)
    with modifier_engine.connect() as connection:
        # not using a context manager for managing the transaction here because
        # we need explicit control over the clean up action, since it shall
        # be performed by another DB user
        transaction = connection.begin()
        try:
            insert_query = sla.text(
                f'INSERT INTO {public_table_name} (road_name, geom) VALUES (:name, ST_GeomFromText(:geom, 4326))'
            )
            insert_result = connection.execute(
                insert_query,
                name='dummy',
                geom='LINESTRING(-71.160 42.258, -71.160 42.259, -71.161 42.25)'
            )
        except exc.ProgrammingError:
            transaction.rollback()
            raise
        finally:  # clean up the DB
            with creator_engine.connect() as connection:
                connection.execute(f'DROP table {public_table_name}')


@pytest.mark.parametrize('creator_username, modifier_username, schemaname', [
    pytest.param('ppd_editor1', 'ppd_user1', 'ppd_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t75.1'),
    pytest.param('ppd_editor1', 'ppd_user1', 'dominode_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t75.2'),
    pytest.param('ppd_editor1', 'lsd_user1', 'dominode_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='75.3'),
])
def test_regular_user_cannot_update_features_on_table_in_public_schema(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        creator_username,
        modifier_username,
        schemaname,
):
    creator_engine = _connect_to_db(creator_username, db_admin_credentials, db_users_credentials)
    creator_department = creator_username.partition('_')[0]
    table_name = f'{schemaname}."{creator_department}_roads_v0.0.1"'
    with creator_engine.connect() as connection:
        with connection.begin() as transaction:
            create_result = connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
            insert_query = sla.text(
                f'INSERT INTO {table_name} (road_name, geom) VALUES (:name, ST_GeomFromText(:geom, 4326))'
            )
            original_name = 'dummy'
            insert_result = connection.execute(
                insert_query,
                name=original_name,
                geom='LINESTRING(-71.160 42.258, -71.160 42.259, -71.161 42.25)'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeMoveTableToPublicSchema(\'{table_name}\')')
            )
    public_table_name = table_name.replace(schemaname, 'public')
    modifier_engine = _connect_to_db(modifier_username, db_admin_credentials, db_users_credentials)
    with modifier_engine.connect() as connection:
        # not using a context manager for managing the transaction here because
        # we need explicit control over the clean up action, since it shall
        # be performed by another DB user
        transaction = connection.begin()
        try:
            update_query = sla.text(
                f'UPDATE {public_table_name} SET road_name = :new_name WHERE road_name = :original_name'
            )
            update_result = connection.execute(
                update_query,
                new_name='new_dummy',
                original_name=original_name
            )
        except exc.ProgrammingError:
            transaction.rollback()
            raise
        finally:  # clean up the DB
            with creator_engine.connect() as connection:
                connection.execute(f'DROP table {public_table_name}')


@pytest.mark.parametrize('creator_username, modifier_username, schemaname', [
    pytest.param('ppd_editor1', 'ppd_user2', 'ppd_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t69.1'),
    pytest.param('ppd_editor1', 'ppd_user2', 'dominode_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t69.2'),
    pytest.param('ppd_editor1', 'lsd_user2', 'dominode_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t69.3'),
])
def test_regular_user_cannot_delete_table_in_public_schema(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        creator_username,
        modifier_username,
        schemaname
):
    creator_engine = _connect_to_db(creator_username, db_admin_credentials, db_users_credentials)
    creator_department = creator_username.partition('_')[0]
    table_name = f'{schemaname}."{creator_department}_roads_v0.0.1"'
    with creator_engine.connect() as connection:
        with connection.begin() as transaction:
            create_result = connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeMoveTableToPublicSchema(\'{table_name}\')')
            )
    public_table_name = table_name.replace(schemaname, 'public')
    modifier_engine = _connect_to_db(modifier_username, db_admin_credentials, db_users_credentials)
    with modifier_engine.connect() as connection:
        try:
            connection.execute(f'DROP table {public_table_name}')
        finally:  # clean up the DB
            with creator_engine.connect() as connection:
                connection.execute(f'DROP table {public_table_name}')


@pytest.mark.parametrize('creator_username, modifier_username, schemaname', [
    pytest.param('ppd_editor1', 'ppd_user1', 'ppd_staging', id='t71.1'),
    pytest.param('ppd_editor1', 'ppd_user1', 'dominode_staging', id='t71.2'),
    pytest.param('ppd_editor1', 'lsd_user1', 'ppd_staging', id='t71.3'),
    pytest.param('ppd_editor1', 'lsd_user1', 'dominode_staging', id='t71.4'),
    pytest.param('ppd_editor1', 'ppd_editor1', 'ppd_staging', id='t72.1'),
    pytest.param('ppd_editor1', 'ppd_editor1', 'dominode_staging', id='t72.2'),
    pytest.param('ppd_editor1', 'lsd_editor1', 'ppd_staging', id='t72.3'),
    pytest.param('ppd_editor1', 'lsd_editor1', 'dominode_staging', id='t72.4'),
])
def test_user_can_select_features_in_public_schema(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        creator_username,
        modifier_username,
        schemaname,

):
    creator_engine = _connect_to_db(creator_username, db_admin_credentials, db_users_credentials)
    creator_department = creator_username.partition('_')[0]
    table_name = f'{schemaname}."{creator_department}_roads_v0.0.1"'
    with creator_engine.connect() as connection:
        with connection.begin() as transaction:
            create_result = connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            insert_query = sla.text(
                f'INSERT INTO {table_name} (road_name, geom) VALUES (:name, ST_GeomFromText(:geom, 4326))'
            )
            insert_result = connection.execute(
                insert_query,
                name='dummy',
                geom='LINESTRING(-71.160 42.258, -71.160 42.259, -71.161 42.25)'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeMoveTableToPublicSchema(\'{table_name}\')')
            )
    public_table_name = table_name.replace(schemaname, 'public')
    modifier_engine = _connect_to_db(modifier_username, db_admin_credentials, db_users_credentials)
    with modifier_engine.connect() as connection:
        # not using a context manager for managing the transaction here because
        # we need explicit control over the clean up action, since it shall
        # be performed by another DB user
        select_result = connection.execute(sla.text(f'SELECT * FROM {public_table_name}'))
        assert len(select_result.fetchall()) > 0
    with creator_engine.connect() as connection:
        connection.execute(f'DROP table {public_table_name}')


@pytest.mark.parametrize('creator_username, modifier_username, schemaname', [
    pytest.param('ppd_editor1', 'ppd_user2', 'ppd_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t77.1'),
    pytest.param('ppd_editor1', 'ppd_user2', 'dominode_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t77.2'),
    pytest.param('ppd_editor1', 'lsd_user2', 'dominode_staging', marks=pytest.mark.raises(exception=exc.ProgrammingError), id='t77.3'),
])
def test_regular_user_cannot_delete_features_from_table_in_public_schema(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        creator_username,
        modifier_username,
        schemaname
):
    creator_engine = _connect_to_db(creator_username, db_admin_credentials, db_users_credentials)
    creator_department = creator_username.partition('_')[0]
    table_name = f'{schemaname}."{creator_department}_roads_v0.0.1"'
    with creator_engine.connect() as connection:
        with connection.begin() as transaction:
            create_result = connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            insert_query = sla.text(
                f'INSERT INTO {table_name} (road_name, geom) VALUES (:name, ST_GeomFromText(:geom, 4326))'
            )
            original_name = 'dummy'
            insert_result = connection.execute(
                insert_query,
                name=original_name,
                geom='LINESTRING(-71.160 42.258, -71.160 42.259, -71.161 42.25)'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeMoveTableToPublicSchema(\'{table_name}\')')
            )
    public_table_name = table_name.replace(schemaname, 'public')
    modifier_engine = _connect_to_db(modifier_username, db_admin_credentials, db_users_credentials)
    with modifier_engine.connect() as connection:
        try:
            update_query = sla.text(
                f'DELETE FROM {public_table_name} WHERE road_name = :original_name'
            )
            update_result = connection.execute(
                update_query,
                original_name=original_name
            )
        finally:  # clean up the DB
            with creator_engine.connect() as connection:
                connection.execute(f'DROP table {public_table_name}')


@pytest.mark.parametrize('creator_username, modifier_username, initial_schemaname, final_schemaname', [
    pytest.param('ppd_editor1', 'ppd_user2', 'ppd_staging', 'ppd_staging', id='t79.1'),
    pytest.param('ppd_editor1', 'ppd_editor2', 'ppd_staging', 'ppd_staging', id='t80.1'),
    pytest.param('ppd_editor1', 'ppd_user2', 'dominode_staging', 'ppd_staging', id='t79.2'),
    pytest.param('ppd_editor1', 'ppd_editor2', 'dominode_staging', 'ppd_staging', id='t80.2'),
    pytest.param('ppd_editor1', 'lsd_user2', 'ppd_staging', 'lsd_staging', id='t81.1'),
    pytest.param('ppd_editor1', 'lsd_user2', 'dominode_staging', 'lsd_staging', id='t81.1'),
    pytest.param('ppd_editor1', 'lsd_editor2', 'ppd_staging', 'lsd_staging', id='t82.1'),
    pytest.param('ppd_editor1', 'lsd_editor2', 'dominode_staging', 'lsd_staging', id='t82.2'),
])
def test_regular_user_can_call_copyTableBackToStagingSchema(
        db_users,
        db_admin_credentials,
        db_users_credentials,
        creator_username,
        modifier_username,
        initial_schemaname,
        final_schemaname,
):
    creator_engine = _connect_to_db(creator_username, db_admin_credentials, db_users_credentials)
    creator_department = creator_username.partition('_')[0]
    table_name = f'{initial_schemaname}."{creator_department}_roads_v0.0.1"'
    with creator_engine.connect() as connection:
        with connection.begin() as transaction:
            create_result = connection.execute(
                f'CREATE TABLE {table_name} '
                f'(id serial, road_name text, geom geometry(LINESTRING, 4326))'
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeSetStagingPermissions(\'{table_name}\')')
            )
            connection.execute(
                sla.text(f'SELECT DomiNodeMoveTableToPublicSchema(\'{table_name}\')')
            )
    public_table_name = table_name.replace(initial_schemaname, 'public')
    copied_table_name = public_table_name.replace(
        'public', final_schemaname).replace('v0.0.1', 'v0.0.2')
    modifier_engine = _connect_to_db(modifier_username, db_admin_credentials, db_users_credentials)
    with modifier_engine.connect() as connection:
        connection.execute(
            sla.text(
                f'SELECT DomiNodeCopyTableBackToStagingSchema('
                f'\'{public_table_name}\', \'{copied_table_name}\')'
            )
        )
        connection.execute(f'DROP table {copied_table_name}')  # clean up
    with creator_engine.connect() as connection:
        connection.execute(f'DROP table {public_table_name}')  # clean up


def _connect_to_db(name, db_credentials, users_credentials):
    engine = sla.create_engine(
        f'postgresql://{name}:{users_credentials[name][0]}@'
        f'{db_credentials["host"]}:'
        f'{db_credentials["port"]}/'
        f'{db_credentials["db"]}'
    )
    return engine
