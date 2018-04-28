API.groups.getMembers(group_id=group['gid'], sort="id_asc", offest=i, version = 5.0, timeout=10);
API.users.getSubscriptions(user_id=member, extended = 1, count = 20, version = 5.0, timeout=10);