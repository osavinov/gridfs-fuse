/*
 *  Copyright 2009 Michael Stephens
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef __UTILS_H
#define __UTILS_H

#include <ctime>
#include <string>
#include <cstring>

inline void prepare_path(const char* path, char* filename, char* parent)
{
	int length = strlen(path);
	
	if (length == 1 && path[0] == '/')
	{
		strcpy(parent, "");
		strcpy(filename, "/");
		return;
	}

    int j = 0, file_i = -1, parent_i = 0;
	for (int i = length-1; i>=0; i--)
	{
		if (path[i] == '/')
		{
			file_i = i;
			break;
		}
	}

	if (file_i == -1)
	{
		strcpy(parent, "/");
		strcpy(filename, path);
		return;
	}

    for (int i = file_i-1; i>=0; i--)
    {
        if (path[i] == '/' || i == 0)
        {
            parent_i = i;
            break;
        }
    }

    j = 0;
    for (int i = parent_i+1; i<file_i; i++, j++)
        parent[j] = path[i];
    parent[j] = '\0';
    j = 0;
    for (int i = file_i+1; i<length; i++, j++)
        filename[j] = path[i];
    filename[j] = '\0';

    if (parent[0] == '\0')
    {
        parent[0] = '/';
        parent[1] = '\0';
    }
}

inline const char* fuse_to_mongo_path(const char* path)
{
    if (path[0] == '/')
	{
        return path + 1;
    } 
	else
	{
        return path;
    }
}

inline time_t mongo_time_to_unix_time(unsigned long long mtime)
{
    return mtime / 1000;
}

inline time_t unix_time_to_mongo_time(unsigned long long utime)
{
    return utime * 1000;
}

inline time_t mongo_time()
{
    return unix_time_to_mongo_time(time(NULL));
}

inline std::string namespace_xattr(const std::string name)
{
#ifdef __linux__
    return "user." + name;
#else
    return name;
#endif
}

inline const char* unnamespace_xattr(const char* name)
{
#ifdef __linux__
    if (std::strstr(name, "user.") == name)
    {
        return name + 5;
    }
    else
    {
        return NULL;
    }
#else
    return name;
#endif
}

#endif
