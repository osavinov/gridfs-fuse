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

#include "operations.h"
#include "options.h"
#include "utils.h"
#include "local_gridfile.h"
#include <algorithm>
#include <cstring>
#include <cerrno>
#include <fcntl.h>

#include <mongo/client/dbclient.h>
#include <mongo/client/gridfs.h>
#include <mongo/client/connpool.h>

#include <stdio.h>
#ifdef __linux__
#include <attr/xattr.h>
#endif

#include "log.h"
#include "params.h"

using namespace std;
using namespace mongo;

std::multimap<string, LocalGridFile*> open_files;

unsigned int FH = 1;

static int gridfs_error(char *str)
{
    int ret = -errno;

    log_msg("    ERROR %s: %s\n", str, strerror(errno));

    return ret;
}

void init_root()
{
	open_files.insert( pair<string, LocalGridFile*>("/", new LocalGridFile(DEFAULT_CHUNK_SIZE, "", true)) );
	ScopedDbConnection conn(gridfs_options.host);
	BSONObj p = BSON("_id" << "/" << "parent" << "" << "isDir" << 1);
	conn->insert("db.dirstruct", p);
	conn.done();
}

int gridfs_getattr(const char *path, struct stat *stbuf)
{
	char parent[100], file[100];
	prepare_path(path, file, parent);
    log_msg("\ngridfs_getattr(path=\"%s\", statbuf=0x%08x): file=\"%s\"\n", path, stbuf, file);

    memset(stbuf, 0, sizeof(struct stat));

	pair <multimap<string, LocalGridFile*>::iterator, multimap<string, LocalGridFile*>::iterator> ret;
	ret = open_files.equal_range(file);
	for (multimap<string, LocalGridFile*>::iterator iter = ret.first; iter != ret.second; ++iter)
	{
		if (strcmp(iter->second->_parent, parent) == 0)
		{
        	if (iter->second->isDir())// это директория
        	{	
        		log_msg("It's directory!\n");
    			stbuf->st_mode = S_IFDIR | 0777;
            	stbuf->st_nlink = 2;
        	}	
        	else // это одиночный файл
        	{
        		log_msg("Is's single file!\n");
        		stbuf->st_mode = S_IFREG | 0555;
        		stbuf->st_nlink = 1;
        		stbuf->st_ctime = time(NULL);
        		stbuf->st_mtime = time(NULL);
        		stbuf->st_size = iter->second->getLength();
        	}
        	return 0;
		}
    }	

    ScopedDbConnection conn(gridfs_options.host);
	auto_ptr<DBClientCursor> cursor = conn->query("db.dirstruct", QUERY("_id" << file));
	
    GridFS gf(conn.conn(), gridfs_options.db);
    GridFile gridfile = gf.findFile(file);

	if (gridfile.exists() && !cursor->more())
	{
		BSONObj p = BSON("_id" << file << "parent" << parent << "isDir" << 0);
	    conn->insert("db.dirstruct", p);
		log_msg("Is's single file!\n");
	}
	else if (!gridfile.exists() && cursor->more())
	{
		BSONObj p = cursor->next();
		if (p.getIntField("isDir") == 1)
		{
			log_msg("It's directory!\n");
			stbuf->st_mode = S_IFDIR | 0777;
			stbuf->st_nlink = 2;
			return 0;
		}
		else
		{
			log_msg("Unexpected record in DB\n");
			return -ENOENT;
		}
	}
	else if (!gridfile.exists() && !cursor->more())
	{
	    log_msg("File is not exists in DB\n");//TODO
		return -ENOENT;
	}
	conn.done();

    stbuf->st_mode = S_IFREG | 0555;
    stbuf->st_nlink = 1;
    stbuf->st_size = gridfile.getContentLength();

    time_t upload_time = mongo_time_to_unix_time(gridfile.getUploadDate());
    stbuf->st_ctime = upload_time;
    stbuf->st_mtime = upload_time;

    return 0;
}

int gridfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                   off_t offset, struct fuse_file_info *fi)
{
	char parent[100], file[100];
	prepare_path(path, file, parent);
    log_msg("\ngridfs_readdir(path=\"%s\", buf=0x%08x, filler=0x%08x, offset=%lld, fi=0x%08x)\n",
	    path, buf, filler, offset, fi);

    filler(buf, ".", NULL, 0);
	log_msg("calling filler with name \".\"\n");
    filler(buf, "..", NULL, 0);
	log_msg("calling filler with name \"..\"\n");

    ScopedDbConnection conn(gridfs_options.host);
	auto_ptr<DBClientCursor> cursor = conn->query("db.dirstruct", QUERY("parent" << file));

	while (cursor->more())
	{
		BSONObj p = cursor->next();
		filler(buf, p.getStringField("_id"), NULL, 0);
		log_msg("calling filler with name \"%s\"\n", p.getStringField("_id"));
	}
	conn.done();

	/*multimap<string, LocalGridFile*>::iterator iter = open_files.find(file);
	pair <multimap<string, LocalGridFile*>::iterator, multimap<string, LocalGridFile*>::iterator> ret;
    ret = open_files.equal_range(file);

	if ( (ret.first == ret.second) && strcmp(iter->second->_parent, parent) )
	{
		log_msg("ERROR: file is not exists!\n");
		return -ENOENT;
	}

	for (multimap<string, LocalGridFile*>::iterator iter = ret.first; iter != ret.second; ++iter)
	{
		if (strcmp(iter->second->_parent, parent) == 0)
		{
			LocalGridFile* lgf = iter->second;

    		for (list<char*>::const_iterator iter = lgf->_childs.begin(); iter != lgf->_childs.end(); iter++)
    		{
        		filler(buf, *iter, NULL, 0);
				log_msg("calling filler with name \"%s\"\n", *iter);
    		}
			return 0;
		}
	}*/
 	
    return 0;
}

int gridfs_open(const char *path, struct fuse_file_info *fi)
{
	char parent[100], file[100];
	prepare_path(path, file, parent);
    log_msg("\ngridfs_open(path\"%s\", fi=0x%08x)\n", path, fi);

    if ((fi->flags & O_ACCMODE) == O_RDONLY)
	{
        if (open_files.find(file) != open_files.end())
		{
            return 0;
        }

        ScopedDbConnection sdc(gridfs_options.host);
        GridFS gf(sdc.conn(), gridfs_options.db);
        GridFile gridfile = gf.findFile(file);
        sdc.done();

        if(gridfile.exists())
		{
            return 0;
        }

        return -ENOENT;
    }
	else
	{
        return -EACCES;
    }
}

int gridfs_create(const char* path, mode_t mode, struct fuse_file_info* ffi)
{
	char parent[100], file[100], fpath[100];
	prepare_path(path, file, parent);

    log_msg("\ngridfs_create(path=\"%s\", mode=0%03o, fi=0x%08x): file=\"%s\", parent=\"%s\"\n", 
		path, mode, ffi, file, parent);

	ScopedDbConnection conn(gridfs_options.host);
	BSONObj p = BSON("_id" << file << "parent" << parent << "isDir" << 0);
	conn->insert("db.dirstruct", p);
	conn.done();

 	open_files.insert( pair<string, LocalGridFile*>(file, new LocalGridFile(DEFAULT_CHUNK_SIZE, parent)) );
    multimap<string,LocalGridFile*>::const_iterator file_iter;
    file_iter = open_files.find(parent);
	if (file_iter == open_files.end())
	{
		log_msg("Cannot find parent=\"%s\"!\n");
	}
    file_iter->second->addChild(file);
	file_iter->second->viewChilds();

    ffi->fh = FH++;

    return 0;
}

int gridfs_release(const char* path, struct fuse_file_info* ffi)
{
	char parent[100], file[100], fpath[100];
	prepare_path(path, file, parent);

    log_msg("\ngridfs_release(path=\"%s\", fi=0x%08x): file=\"%s\", parent=\"%s\"\n",
		path, ffi, file, parent);
			
    if (!ffi->fh)
	{
        // fh is not set if file is opened read only
        // Would check ffi->flags for O_RDONLY instead but MacFuse doesn't
        // seem to properly pass flags into release
        return 0;
    }

    /*delete open_files[file];
    open_files.erase(file);
    map<string,LocalGridFile*>::const_iterator file_iter;
    file_iter = open_files.find(parent);
    file_iter->second->removeChild(file);*/

    return 0;
}

int gridfs_unlink(const char* path)
{
	char parent[100], file[100];
	prepare_path(path, file, parent);
    log_msg("\ngridfs_unlink(path=\"%s\"): file=\"%s\", parent=\"%s\"\n", path, file, parent);

	ScopedDbConnection conn(gridfs_options.host);
	BSONObj p = BSON("_id" << file << "parent" << parent << "isDir" << 0);
	conn->remove("db.dirstruct", p);
	conn.done();

    ScopedDbConnection sdc(gridfs_options.host);
    GridFS gf(sdc.conn(), gridfs_options.db);
    gf.removeFile(file);
    sdc.done();

	pair <multimap<string, LocalGridFile*>::iterator, multimap<string, LocalGridFile*>::iterator> ret;
    ret = open_files.equal_range(file);
	multimap<string, LocalGridFile*>::iterator it = open_files.end();
	for (multimap<string, LocalGridFile*>::iterator iter = ret.first; iter != ret.second; ++iter)
	{
		if (strcmp(iter->second->_parent, parent) == 0)
		{	
			delete iter->second;
			it = iter;
		}
	}	
	if (it != open_files.end())
	{
		open_files.erase(it);
		multimap<string,LocalGridFile*>::const_iterator file_iter;
		file_iter = open_files.find(parent);
		file_iter->second->removeChild(file);

		file_iter->second->viewChilds();
	}

    return 0;
}

int gridfs_read(const char *path, char *buf, size_t size, off_t offset,
                struct fuse_file_info *fi)
{
	char parent[100], file[100];
	prepare_path(path, file, parent);
    log_msg("\ngridfs_read(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n",
    	    path, buf, size, offset, fi);
    size_t len = 0;

	pair <multimap<string, LocalGridFile*>::iterator, multimap<string, LocalGridFile*>::iterator> ret;
	ret = open_files.equal_range(file);
	for (multimap<string, LocalGridFile*>::iterator iter = ret.first; iter != ret.second; ++iter)
	{
		if (strcmp(iter->second->_parent, parent) == 0)
		{
        	LocalGridFile *lgf = iter->second;
        	return lgf->read(buf, size, offset);
		}
	}

	//для файлов, о которых FUSE не знает, но которые есть в базе
    ScopedDbConnection sdc(gridfs_options.host);
    GridFS gf(sdc.conn(), gridfs_options.db);
    GridFile gridfile = gf.findFile(file);

	//если файла в базе нет, выходим
   	if (!gridfile.exists())
	{
        sdc.done();
        return -EBADF;
    }

    int chunk_size = gridfile.getChunkSize();
    int chunk_num = offset / chunk_size;

    while ( (len < size) && (chunk_num < gridfile.getNumChunks()) )
	{
        Chunk chunk = gridfile.getChunk(chunk_num);
        int to_read;
        int cl = chunk.len();

        const char *d = chunk.data(cl);

        if (len)
		{
            to_read = min((long unsigned)cl, (long unsigned)(size - len));
            memcpy(buf + len, d, to_read);
        } 
		else
		{
            to_read = min((long unsigned)(cl - (offset % chunk_size)), (long unsigned)(size - len));
            memcpy(buf + len, d + (offset % chunk_size), to_read);
        }

        len += to_read;
        chunk_num++;
    }

    sdc.done();
    return len;
}

int gridfs_listxattr(const char* path, char* list, size_t size)
{
	char parent[100], file[100];
	prepare_path(path, file, parent);
    log_msg("\ngridfs_listxattr(path=\"%s\", list=0x%08x, size=%d)\n",
	    path, list, size);
    if (open_files.find(file) != open_files.end())
	{
        return 0;
    }

    ScopedDbConnection sdc(gridfs_options.host);
    GridFS gf(sdc.conn(), gridfs_options.db);
    GridFile gridfile = gf.findFile(file);
    sdc.done();

    if (!gridfile.exists())
	{
        return -ENOENT;
    }

    int len = 0;
    BSONObj metadata = gridfile.getMetadata();
    set<string> field_set;
    metadata.getFieldNames(field_set);
    for (set<string>::const_iterator s = field_set.begin(); s != field_set.end(); s++)
	{
        string attr_name = namespace_xattr(*s);
        int field_len = attr_name.size() + 1;
        len += field_len;
        if (size >= len)
		{
            memcpy(list, attr_name.c_str(), field_len);
            list += field_len;
        }
    }

    if (size == 0)
	{
        return len;
    }
	else if (size < len)
	{
        return -ERANGE;
    }

    return len;
}

int gridfs_getxattr(const char* path, const char* name, char* value, size_t size)
{
	if (strcmp(path, "/") == 0)
	{
		return -ENOATTR;
	}

	char parent[100], file[100];
	prepare_path(path, file, parent);
    log_msg("\ngridfs_getxattr(path = \"%s\", name = \"%s\", value = 0x%08x, size = %d)\n",
	    path, name, value, size);

    const char* attr_name = unnamespace_xattr(name);
    if (!attr_name)
	{
        return -ENOATTR;
    }

    if (open_files.find(file) != open_files.end())
	{
        return -ENOATTR;
    }

    ScopedDbConnection sdc(gridfs_options.host);
    GridFS gf(sdc.conn(), gridfs_options.db);
    GridFile gridfile = gf.findFile(file);
    sdc.done();

    if (!gridfile.exists())
	{
        return -ENOENT;
    }

    BSONObj metadata = gridfile.getMetadata();
    if (metadata.isEmpty())
	{
        return -ENOATTR;
    }

    BSONElement field = metadata[attr_name];
    if (field.eoo())
	{
        return -ENOATTR;
    }

    string field_str = field.toString();
    int len = field_str.size() + 1;
    if (size == 0)
	{
        return len;
    } 
	else if(size < len)
	{
        return -ERANGE;
    }

    memcpy(value, field_str.c_str(), len);

    return len;
}

int gridfs_setxattr(const char* path, const char* name, const char* value,
                    size_t size, int flags)
{
    log_msg("\ngridfs_setxattr(path=\"%s\", name=\"%s\", value=\"%s\", size=%d, flags=0x%08x)\n",
	    path, name, value, size, flags);
    return -ENOTSUP;
}

int gridfs_write(const char* path, const char* buf, size_t nbyte,
                 off_t offset, struct fuse_file_info* ffi)
{
	char parent[100], file[100];
	prepare_path(path, file, parent);
    log_msg("\ngridfs_write(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x): file=\"%s\", parent=\"%s\"\n",
	    path, buf, nbyte, offset, ffi, file, parent);

    if (open_files.find(file) == open_files.end())
	{
		log_msg("File is not exists in FUSE!\n");
        return -ENOENT;
    }

	pair <multimap<string, LocalGridFile*>::iterator, multimap<string, LocalGridFile*>::iterator> ret;
    ret = open_files.equal_range(file);
	LocalGridFile *lgf;
	for (multimap<string, LocalGridFile*>::iterator iter = ret.first; iter != ret.second; ++iter)
	{
		if (strcmp(iter->second->_parent, parent) == 0)
		{
			lgf = iter->second;
			log_msg("Writing file=\"%s\" in lgf=0x%08x\n", file, lgf);
		}
	}

    return lgf->write(buf, nbyte, offset);
}

int gridfs_flush(const char* path, struct fuse_file_info *ffi)
{
	char parent[100], file[100];
	prepare_path(path, file, parent);
    log_msg("\ngridfs_flush(path=\"%s\", fi=0x%08x)\n", path, ffi);

    if (!ffi->fh)
	{
        return 0;
    }

    map<string,LocalGridFile*>::iterator file_iter;
    file_iter = open_files.find(file);
    if (file_iter == open_files.end())
	{
        return -ENOENT;
    }

    LocalGridFile *lgf = file_iter->second;

    if (!lgf->dirty())
	{
        return 0;
    }

    ScopedDbConnection sdc(gridfs_options.host);
    DBClientBase &client = sdc.conn();
    GridFS gf(sdc.conn(), gridfs_options.db);

    if (gf.findFile(file).exists())
	{
        gf.removeFile(file);
    }

    size_t len = lgf->getLength();
    char *buf = new char[len];
    lgf->read(buf, len, 0);

    gf.storeFile(buf, len, file);

    sdc.done();

    lgf->flushed();

    return 0;
}

/*int gridfs_rename(const char* old_path, const char* new_path)
{
	//необходимо переделать полностью
    old_path = fuse_to_mongo_path(old_path);
    new_path = fuse_to_mongo_path(new_path);
    log_msg("\ngridfs_rename(fpath=\"%s\", newpath=\"%s\")\n", old_path, new_path);

    ScopedDbConnection sdc(gridfs_options.host);
    DBClientBase &client = sdc.conn();

    string db_name = gridfs_options.db;

    BSONObj file_obj = client.findOne(db_name + ".fs.files",
                                      BSON("filename" << old_path));

    if (file_obj.isEmpty())
	{
        return -ENOENT;
    }

    BSONObjBuilder b;
    set<string> field_names;
    file_obj.getFieldNames(field_names);

    for (set<string>::iterator name = field_names.begin(); name != field_names.end(); name++)
    {
        if (*name != "filename")
		{
            b.append(file_obj.getField(*name));
        }
    }

    b << "filename" << new_path;

    client.update(db_name + ".fs.files",
                  BSON("_id" << file_obj.getField("_id")), b.obj());

    sdc.done();

    return 0;
}*/

int gridfs_mkdir(const char* path, mode_t mode)
{
	char parent[100], file[100];
	prepare_path(path, file, parent);

	log_msg("\ngridfs_mkdir(path=\"%s\", mode=0%03o): file=\"%s\", parent=\"%s\"\n",
		path, mode, file, parent);

	ScopedDbConnection conn(gridfs_options.host);
	BSONObj p = BSON("_id" << file << "parent" << parent << "isDir" << 1);
	conn->insert("db.dirstruct", p);
	conn.done();

    open_files.insert( pair<string, LocalGridFile*>(file, new LocalGridFile(DEFAULT_CHUNK_SIZE, parent, true)) );
	
	multimap<string, LocalGridFile*>::iterator iter = open_files.find(parent);
    iter->second->addChild(file);

    return 0;
}

int gridfs_rmdir(const char* path)
{
	char parent[100], file[100];
	prepare_path(path, file, parent);

    log_msg("\ngridfs_rmdir(path=\"%s\"): file=\"%s\", parent=\"%s\"\n",
		path, file, parent);

	ScopedDbConnection conn(gridfs_options.host);
	BSONObj p = BSON("_id" << file << "parent" << parent);
	conn->remove("db.dirstruct", p);
	conn.done();

    multimap<string,LocalGridFile*>::iterator file_iter;
	file_iter = open_files.find(file);
	if (file_iter == open_files.end())
	{
		return -ENOENT;
	}

	pair <multimap<string, LocalGridFile*>::iterator, multimap<string, LocalGridFile*>::iterator> ret;
	ret = open_files.equal_range(file);
	multimap<string, LocalGridFile*>::iterator it;
	for (multimap<string, LocalGridFile*>::iterator iter = ret.first; iter != ret.second; ++iter)
	{
		if (strcmp(iter->second->_parent, parent) == 0)
		{
			delete iter->second;
			it = iter;
		}
	}
	open_files.erase(it);

	file_iter = open_files.find(parent);
	file_iter->second->removeChild(file);

	return 0;
}
