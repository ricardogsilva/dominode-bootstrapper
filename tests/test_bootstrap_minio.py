import io
import os
import typing
from urllib3.response import HTTPResponse

import pytest
import minio
import minio.error


@pytest.mark.parametrize('access_key, expected_num_buckets', [
    pytest.param('ppd_user1', 3),
    pytest.param('ppd_editor1', 3),
])
def test_regular_user_has_access_to_buckets(
        minio_server_info,
        bootstrapped_minio_server,
        minio_users_credentials,
        access_key,
        expected_num_buckets
):
    minio_client = _get_minio_client(
        access_key, minio_users_credentials, minio_server_info)
    buckets = minio_client.list_buckets()
    print(f'buckets: {buckets}')
    assert len(buckets) == expected_num_buckets


@pytest.mark.parametrize('access_key, bucket', [
    pytest.param('ppd_user1', 'mybucket', marks=pytest.mark.raises(exception=minio.error.AccessDenied))
])
def test_user_cannot_create_new_bucket(
        bootstrapped_minio_server,
        minio_users_credentials,
        minio_server_info,
        access_key,
        bucket
):
    minio_client = _get_minio_client(
        access_key, minio_users_credentials, minio_server_info
    )
    minio_client.make_bucket(bucket_name=bucket)


@pytest.mark.parametrize('access_key, bucket', [
    pytest.param('ppd_user1', 'ppd-staging', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_editor1', 'ppd-staging', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_user1', 'dominode-staging', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_editor1', 'dominode-staging', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_user1', 'public', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_editor1', 'public', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
])
def test_user_cannot_delete_bucket(
        bootstrapped_minio_server,
        minio_users_credentials,
        minio_server_info,
        access_key,
        bucket
):
    minio_client = _get_minio_client(
        access_key, minio_users_credentials, minio_server_info
    )
    minio_client.remove_bucket(bucket)


@pytest.mark.parametrize('access_key, bucket_path', [
    pytest.param('ppd_user1', 'ppd-staging/file1.txt', id='t91.1'),
    pytest.param('ppd_user1', 'ppd-staging/somedir/file2.txt', id='t83, t91.2'),
    pytest.param('ppd_editor1', 'ppd-staging/file3.txt', id='t92.1'),
    pytest.param('ppd_editor1', 'ppd-staging/somedir/file4.txt', id='t84, t92.2'),
    pytest.param('ppd_user1', 'dominode-staging/ppd/file1.txt', id='t119.1'),
    pytest.param('ppd_user1', 'dominode-staging/ppd/somedir/file2.txt', id='t111, t119.2'),
    pytest.param('ppd_editor1', 'dominode-staging/ppd/file3.txt', id='t120.1'),
    pytest.param('ppd_editor1', 'dominode-staging/ppd/somedir/file4.txt', id='t112, t120.2'),
    pytest.param('ppd_editor1', 'public/ppd/file3.txt', id='t148.1'),
    pytest.param('ppd_editor1', 'public/ppd/somedir/file4.txt', id='t140, t148.2'),
])
def test_user_is_able_to_put_file_in_bucket(
        bootstrapped_minio_server,
        minio_users_credentials: typing.Dict,
        minio_server_info: typing.Dict,
        minio_admin_client: minio.Minio,
        access_key: str,
        bucket_path: str,
):
    minio_client = _get_minio_client(
        access_key, minio_users_credentials, minio_server_info
    )
    bucket_name, object_path = bucket_path.partition('/')[::2]
    _create_file(minio_client, bucket_name, object_path)
    minio_admin_client.remove_object(bucket_name, object_path)


@pytest.mark.parametrize('access_key, bucket_path', [
    pytest.param('ppd_user1', 'ppd-staging/file1.txt', id='t93.1'),
    pytest.param('ppd_user1', 'ppd-staging/somedir/file2.txt', id='t85, t93.2'),
    pytest.param('ppd_editor1', 'ppd-staging/file3.txt', id='t94.1'),
    pytest.param('ppd_editor1', 'ppd-staging/somedir/file4.txt', id='t86, t94.2'),
    pytest.param('ppd_user1', 'dominode-staging/ppd/file1.txt', id='t121.1'),
    pytest.param('ppd_user1', 'dominode-staging/ppd/somedir/file2.txt', id='t113, t121.2'),
    pytest.param('ppd_editor1', 'dominode-staging/ppd/file3.txt', id='t122.1'),
    pytest.param('ppd_editor1', 'dominode-staging/ppd/somedir/file4.txt', id='t114, t122.2'),
    pytest.param('ppd_editor1', 'public/ppd/file3.txt', id='t150.1'),
    pytest.param('ppd_editor1', 'public/ppd/somedir/file4.txt', id='t142, t150.2'),
])
def test_user_is_able_to_delete_file_from_bucket(
        bootstrapped_minio_server,
        minio_users_credentials: typing.Dict,
        minio_server_info: typing.Dict,
        access_key: str,
        bucket_path: str
):
    minio_client = _get_minio_client(
        access_key, minio_users_credentials, minio_server_info
    )
    bucket_name, object_path = bucket_path.partition('/')[::2]
    _create_file(minio_client, bucket_name, object_path)
    minio_client.remove_object(bucket_name, object_path)


@pytest.mark.parametrize('creator, accessor, bucket_path', [
    pytest.param('ppd_user1', 'ppd_user2', 'ppd-staging/file1.txt', id='t89.1'),
    pytest.param('ppd_editor1', 'ppd_user2', 'ppd-staging/file2.txt', id='t89.2'),
    pytest.param('ppd_user1', 'ppd_editor1', 'ppd-staging/file3.txt', id='t90.1'),
    pytest.param('ppd_editor1', 'ppd_editor2', 'ppd-staging/file4.txt', id='t90.2'),
    pytest.param('ppd_user1', 'ppd_user2', 'dominode-staging/ppd/file5.txt', id='t117.1'),
    pytest.param('ppd_editor1', 'ppd_user2', 'dominode-staging/ppd/file6.txt', id='t117.2'),
    pytest.param('ppd_editor1', 'ppd_editor2', 'dominode-staging/ppd/file7.txt', id='t118.1'),
    pytest.param('ppd_user1', 'lsd_user1', 'dominode-staging/ppd/file8.txt', id='t131'),
    pytest.param('ppd_user1', 'lsd_editor1', 'dominode-staging/ppd/file9.txt', id='t132'),
    pytest.param('ppd_editor1', 'ppd_user1', 'public/ppd/file10.txt', id='t145'),
    pytest.param('ppd_editor1', 'ppd_editor2', 'public/ppd/file11.txt', id='t146'),
    pytest.param('ppd_editor1', 'lsd_user1', 'public/ppd/file12.txt', id='t159'),
    pytest.param('ppd_editor1', 'lsd_editor1', 'public/ppd/file13.txt', id='t160'),
])
def test_user_is_able_to_access_file(
        bootstrapped_minio_server,
        minio_users_credentials: typing.Dict,
        minio_server_info: typing.Dict,
        creator: str,
        accessor: str,
        bucket_path: str
):
    creator_minio_client = _get_minio_client(
        creator, minio_users_credentials, minio_server_info)
    bucket_name, object_path = bucket_path.partition('/')[::2]
    contents = 'hello world'
    _create_file(
        creator_minio_client, bucket_name, object_path, contents=contents)
    accessor_minio_client = _get_minio_client(
        accessor, minio_users_credentials, minio_server_info)
    data: HTTPResponse = accessor_minio_client.get_object(bucket_name, object_path)
    buffer = io.StringIO()
    for chunk in data.stream(32 * 1024):
        buffer.write(chunk.decode('utf-8'))
    buffer.seek(0)
    response = buffer.read()
    assert response == contents
    creator_minio_client.remove_object(bucket_name, object_path)


@pytest.mark.parametrize('creator, modifier, creation_bucket_path, modified_path', [
    pytest.param('ppd_user1', 'ppd_user2', 'ppd-staging/file1.txt', 'ppd-staging/extra/file1.txt', id='t87'),
    pytest.param('ppd_user1', 'ppd_editor1', 'ppd-staging/file1.txt', 'ppd-staging/extra/file1.txt', id='t88'),
    pytest.param('ppd_user1', 'ppd_user2', 'dominode-staging/ppd/file1.txt', 'dominode-staging/ppd/extra/file1.txt', id='t115'),
    pytest.param('ppd_user1', 'ppd_editor1', 'dominode-staging/ppd/file1.txt', 'dominode-staging/ppd/extra/file1.txt', id='t116'),
    pytest.param('ppd_editor1', 'ppd_editor2', 'public/ppd/file1.txt', 'public/ppd/extra/file1.txt', id='t144'),
])
def test_user_is_able_to_rename_file_directory(
        bootstrapped_minio_server,
        minio_users_credentials: typing.Dict,
        minio_server_info: typing.Dict,
        creator: str,
        modifier: str,
        creation_bucket_path: str,
        modified_path: str,
):
    """
    Test is actually seeing if user is able to copy an object

    minIO does not seem to have a `rename` operation - what we are doing
    is copying the object to the specified location and then deleting the
    original one.

    """

    creator_minio_client = _get_minio_client(
        creator, minio_users_credentials, minio_server_info)
    creation_bucket_name, creation_object_path = creation_bucket_path.partition(
        '/')[::2]
    contents = 'hello world'
    _create_file(
        creator_minio_client,
        creation_bucket_name,
        creation_object_path,
        contents=contents
    )

    modifier_minio_client = _get_minio_client(
        modifier, minio_users_credentials, minio_server_info)
    modified_bucket_name, modified_object_path = modified_path.partition(
        '/')[::2]
    try:
        modifier_minio_client.copy_object(
            modified_bucket_name,
            modified_object_path,
            creation_bucket_path
        )
        modifier_minio_client.remove_object(creation_bucket_name, creation_object_path)
        modifier_minio_client.remove_object(modified_bucket_name, modified_object_path)
    except minio.error.ResponseError:
        creator_minio_client.remove_object(creation_bucket_name, creation_object_path)


@pytest.mark.parametrize('creator, modifier, bucket_path', [
    pytest.param('ppd_user1', 'ppd_user2', 'ppd-staging/file1.txt', id='t95.1'),
    pytest.param('ppd_editor1', 'ppd_user2', 'ppd-staging/file1.txt', id='t95.2'),
    pytest.param('ppd_user1', 'ppd_editor1', 'ppd-staging/file1.txt', id='t96.1'),
    pytest.param('ppd_editor1', 'ppd_editor2', 'ppd-staging/file1.txt', id='t96.1'),
    pytest.param('ppd_user1', 'ppd_user2', 'dominode-staging/ppd/file1.txt', id='t123.1'),
    pytest.param('ppd_editor1', 'ppd_user2', 'dominode-staging/ppd/file1.txt', id='t123.2'),
    pytest.param('ppd_user1', 'ppd_editor1', 'dominode-staging/ppd/file1.txt', id='t124.1'),
    pytest.param('ppd_editor1', 'ppd_editor2', 'dominode-staging/ppd/file1.txt', id='t124.2'),
    pytest.param('ppd_editor1', 'ppd_editor2', 'public/ppd/file1.txt', id='t152'),
])
def test_user_is_able_to_edit_file(
        bootstrapped_minio_server,
        minio_users_credentials: typing.Dict,
        minio_server_info: typing.Dict,
        creator: str,
        modifier: str,
        bucket_path: str,
):
    """Test checks if user can put object with same name as a previous one.

    minIO does not seem to have a `edit` operation - what we are doing
    is putting a different file into a previously existing one.

    """

    creator_minio_client = _get_minio_client(
        creator, minio_users_credentials, minio_server_info)
    bucket_name, object_path = bucket_path.partition('/')[::2]
    initial_contents = 'hello world'
    _create_file(
        creator_minio_client,
        bucket_name,
        object_path,
        contents=initial_contents
    )
    modified_contents = 'This was changed'
    modifier_minio_client = _get_minio_client(
        modifier, minio_users_credentials, minio_server_info)
    try:
        _create_file(
            modifier_minio_client,
            bucket_name,
            object_path,
            contents=modified_contents
        )
        data: HTTPResponse = modifier_minio_client.get_object(
            bucket_name, object_path)
        buffer = io.StringIO()
        for chunk in data.stream(32 * 1024):
            buffer.write(chunk.decode('utf-8'))
        buffer.seek(0)
        response = buffer.read()
        assert response == modified_contents
    finally:
        creator_minio_client.remove_object(bucket_name, object_path)


def _create_file(
        minio_client: minio.Minio,
        bucket_name: str,
        object_path: str,
        contents: typing.Optional[str] = 'this is some sample content'
):

    buffer = io.BytesIO(bytes(contents, 'utf-8'))
    buffer.seek(0, os.SEEK_END)
    buffer_size = buffer.tell()
    buffer.seek(0)
    print(f'bucket_name: {bucket_name}')
    print(f'object_path: {object_path}')
    print(f'contents: {contents}')
    print(f'buffer_size: {buffer_size}')
    minio_client.put_object(
        bucket_name,
        object_path,
        buffer,
        buffer_size
    )


@pytest.mark.parametrize('access_key, bucket_path', [
    pytest.param('ppd_user1', 'lsd-staging/file1.txt', id='t105.1', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_user1', 'lsd-staging/somedir/file2.txt', id='t97, t105.2', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_editor1', 'lsd-staging/file3.txt', id='t106.1', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_editor1', 'lsd-staging/somedir/file4.txt', id='t98, t106.2', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_user1', 'dominode-staging/lsd/file1.txt', id='t133.1', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_user1', 'dominode-staging/lsd/somedir/file2.txt', id='t125, t133.2', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_editor1', 'dominode-staging/lsd/file3.txt', id='t134.1', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_editor1', 'dominode-staging/lsd/somedir/file4.txt', id='t126, t134.2', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),

    pytest.param('ppd_user1', 'public/ppd/file3.txt', id='t147.1', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_user1', 'public/ppd/somedir/file3.txt', id='t139, t147.2', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),

    pytest.param('ppd_user1', 'public/lsd/file3.txt', id='t161.1', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_user1', 'public/lsd/somedir/file4.txt', id='t153, t161.2', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),

    pytest.param('ppd_editor1', 'public/lsd/file3.txt', id='t162.1', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_editor1', 'public/lsd/somedir/file4.txt', id='t154, t162.2', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),

])
def test_user_is_not_able_to_put_file_in_bucket_owned_by_another_department(
        bootstrapped_minio_server,
        minio_users_credentials: typing.Dict,
        minio_server_info: typing.Dict,
        minio_admin_client: minio.Minio,
        access_key: str,
        bucket_path: str,
):
    minio_client = _get_minio_client(
        access_key, minio_users_credentials, minio_server_info
    )
    bucket_name, object_path = bucket_path.partition('/')[::2]
    _create_file(minio_client, bucket_name, object_path)


@pytest.mark.parametrize('creator, modifier, bucket_path', [
    pytest.param('ppd_user1', 'lsd_user1', 'ppd-staging/file1.txt', id='t107.1', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_user1', 'lsd_user1', 'ppd-staging/somedir/file2.txt', id='t99, t107.2', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_editor1', 'lsd_editor1', 'ppd-staging/file3.txt', id='t108.1', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_editor1', 'lsd_editor1', 'ppd-staging/somedir/file4.txt', id='t100, t108.2', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_user1', 'lsd_user1', 'dominode-staging/ppd/file1.txt', id='t135.1', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_user1', 'lsd_user1', 'dominode-staging/ppd/somedir/file2.txt', id='t127, t135.2', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_editor1', 'lsd_editor1', 'dominode-staging/ppd/file3.txt', id='t136.1', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_editor1', 'lsd_editor1', 'dominode-staging/ppd/somedir/file4.txt', id='t128, t136.2', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_editor1', 'ppd_user1', 'public/ppd/file3.txt', id='t149.1', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_editor1', 'ppd_user1', 'public/ppd/somedir/file4.txt', id='t141, t149.2', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_editor1', 'lsd_user1', 'public/ppd/file3.txt', id='t163.1', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_editor1', 'lsd_editor1', 'public/ppd/file3.txt', id='t164.1', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_editor1', 'lsd_user1', 'public/ppd/somedir/file4.txt', id='t155, t163.2', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_editor1', 'lsd_editor1', 'public/ppd/somedir/file4.txt', id='t156, t164.2', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
])
def test_user_is_not_able_to_delete_file_from_bucket_owned_by_another_department(
        bootstrapped_minio_server,
        minio_users_credentials: typing.Dict,
        minio_server_info: typing.Dict,
        creator: str,
        modifier: str,
        bucket_path: str
):
    creator_minio_client = _get_minio_client(
        creator, minio_users_credentials, minio_server_info
    )
    bucket_name, object_path = bucket_path.partition('/')[::2]
    _create_file(creator_minio_client, bucket_name, object_path)
    modifier_minio_client = _get_minio_client(
        modifier, minio_users_credentials, minio_server_info
    )
    try:
        modifier_minio_client.remove_object(bucket_name, object_path)
    finally:
        creator_minio_client.remove_object(bucket_name, object_path)


@pytest.mark.parametrize('creator, modifier, creation_bucket_path, modified_path', [
    pytest.param('ppd_user1', 'lsd_user1', 'ppd-staging/file1.txt', 'ppd-staging/extra/file1.txt', id='t101', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_user1', 'lsd_editor1', 'ppd-staging/file1.txt', 'ppd-staging/extra/file1.txt', id='t102', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_user1', 'lsd_user1', 'dominode-staging/ppd/file1.txt', 'dominode-staging/ppd/extra/file1.txt', id='t129', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_user1', 'lsd_editor1', 'dominode-staging/ppd/file1.txt', 'dominode-staging/ppd/extra/file1.txt', id='t130', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),

    pytest.param('ppd_editor1', 'ppd_user1', 'public/ppd/file1.txt', 'public/ppd/extra/file1.txt', id='t143', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),

    pytest.param('ppd_editor1', 'lsd_user1', 'public/ppd/file1.txt', 'public/ppd/extra/file1.txt', id='t157', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_editor1', 'lsd_editor1', 'public/ppd/file1.txt', 'public/ppd/extra/file1.txt', id='t158', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),

])
def test_user_is_not_able_to_rename_file_directory_on_buckets_owned_by_another_department(
        bootstrapped_minio_server,
        minio_users_credentials: typing.Dict,
        minio_server_info: typing.Dict,
        creator: str,
        modifier: str,
        creation_bucket_path: str,
        modified_path: str,
):
    """
    Test is actually checking that user is not able to copy an object

    minIO does not seem to have a `rename` operation - what we are doing
    is copying the object to the specified location and then deleting the
    original one.

    """

    creator_minio_client = _get_minio_client(
        creator, minio_users_credentials, minio_server_info)
    creation_bucket_name, creation_object_path = creation_bucket_path.partition(
        '/')[::2]
    contents = 'hello world'
    _create_file(
        creator_minio_client,
        creation_bucket_name,
        creation_object_path,
        contents=contents
    )

    modifier_minio_client = _get_minio_client(
        modifier, minio_users_credentials, minio_server_info)
    modified_bucket_name, modified_object_path = modified_path.partition(
        '/')[::2]
    try:
        modifier_minio_client.copy_object(
            modified_bucket_name,
            modified_object_path,
            creation_bucket_path
        )
    finally:
        creator_minio_client.remove_object(
            creation_bucket_name, creation_bucket_path)


@pytest.mark.parametrize('creator, accessor, bucket_path', [
    pytest.param('ppd_user1', 'lsd_user1', 'ppd-staging/file1.txt', id='t103.1', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_editor1', 'lsd_user1', 'ppd-staging/file2.txt', id='t103.2', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_user1', 'lsd_editor1', 'ppd-staging/file3.txt', id='t104.1', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_editor1', 'lsd_editor2', 'ppd-staging/file4.txt', id='t104.2', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
])
def test_user_is_not_able_to_access_file_on_buckets_owned_by_another_department(
        bootstrapped_minio_server,
        minio_users_credentials: typing.Dict,
        minio_server_info: typing.Dict,
        creator: str,
        accessor: str,
        bucket_path: str
):
    creator_minio_client = _get_minio_client(
        creator, minio_users_credentials, minio_server_info)
    bucket_name, object_path = bucket_path.partition('/')[::2]
    contents = 'hello world'
    _create_file(
        creator_minio_client, bucket_name, object_path, contents=contents)
    accessor_minio_client = _get_minio_client(
        accessor, minio_users_credentials, minio_server_info)
    try:
        data: HTTPResponse = accessor_minio_client.get_object(
            bucket_name, object_path)
    finally:
        creator_minio_client.remove_object(bucket_name, object_path)


@pytest.mark.parametrize('creator, modifier, bucket_path', [
    pytest.param('ppd_user1', 'lsd_user1', 'ppd-staging/file1.txt', id='t109.1', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_editor1', 'lsd_user1', 'ppd-staging/file1.txt', id='t109.2', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_user1', 'lsd_editor1', 'ppd-staging/file1.txt', id='t110.1', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_editor1', 'lsd_editor1', 'ppd-staging/file1.txt', id='t110.1', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_user1', 'lsd_user1', 'dominode-staging/ppd/file1.txt', id='t137.1', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_editor1', 'lsd_user1', 'dominode-staging/ppd/file1.txt', id='t137.2', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_user1', 'lsd_editor1', 'dominode-staging/ppd/file1.txt', id='t138.1', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_editor1', 'lsd_editor1', 'dominode-staging/ppd/file1.txt', id='t138.2', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_editor1', 'ppd_user1', 'public/ppd/file1.txt', id='t151', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_editor1', 'lsd_user1', 'public/ppd/file1.txt', id='t165', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),
    pytest.param('ppd_editor1', 'lsd_editor1', 'public/ppd/file1.txt', id='t166', marks=pytest.mark.raises(exception=minio.error.AccessDenied)),

])
def test_user_is_not_able_to_edit_file(
        bootstrapped_minio_server,
        minio_users_credentials: typing.Dict,
        minio_server_info: typing.Dict,
        creator: str,
        modifier: str,
        bucket_path: str,
):
    """Test checks that user cannot put object with same name as a previous one.

    minIO does not seem to have a `edit` operation - what we are doing
    is putting a different file into a previously existing one.

    """

    creator_minio_client = _get_minio_client(
        creator, minio_users_credentials, minio_server_info)
    bucket_name, object_path = bucket_path.partition('/')[::2]
    initial_contents = 'hello world'
    _create_file(
        creator_minio_client,
        bucket_name,
        object_path,
        contents=initial_contents
    )
    modified_contents = 'This was changed'
    modifier_minio_client = _get_minio_client(
        modifier, minio_users_credentials, minio_server_info)
    try:
        _create_file(
            modifier_minio_client,
            bucket_name,
            object_path,
            contents=modified_contents
        )
    finally:
        creator_minio_client.remove_object(bucket_name, object_path)


def _get_minio_client(
        access_key: str,
        minio_users_credentials: typing.Dict,
        minio_server_info: typing.Dict,
):
    secret_key = minio_users_credentials[access_key][0]
    endpoint = f'localhost:{minio_server_info["port"]}'
    return minio.Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )
