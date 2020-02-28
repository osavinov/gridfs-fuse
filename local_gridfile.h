#ifndef _LOCAL_GRIDFILE_H
#define _LOCAL_GRIDFILE_H

#include <vector>
#include <list>
#include <cstring>
#include <iostream>

const unsigned int DEFAULT_CHUNK_SIZE = 256 * 1024;

class LocalGridFile
{
public:
    LocalGridFile(int chunkSize = DEFAULT_CHUNK_SIZE, const char* parent = NULL, bool isDir = false) :
    _chunkSize(chunkSize), _length(0), _dirty(true)
	{
    	_chunks.push_back(new char[_chunkSize]);
        _isDir = isDir;
        strcpy(_parent, parent);
    }

    ~LocalGridFile()
	{
        for (std::vector<char*>::iterator i = _chunks.begin(); i != _chunks.end(); ++i)
            delete *i;
        for (std::list<char*>::iterator i = _childs.begin(); i != _childs.end(); ++i)
        	delete *i;
    }

    int getChunkSize() { return _chunkSize; }
    int getNumChunks() { return _chunks.size(); }
    int getLength() { return _length; }
    char* getChunk(int n) { return _chunks[n]; }
    bool dirty() { return _dirty; }
    bool isDir() { return _isDir; }
    void flushed() { _dirty = false; }

    int write(const char* buf, size_t nbyte, off_t offset);
    int read(char* buf, size_t size, off_t offset);

    void addChild(const char* name);
    void removeChild(const char* name);
	void viewChilds();

private:
    int _chunkSize, _length;
    bool _dirty;
    bool _isDir;
    std::vector<char*> _chunks;
public:
	char _parent[100];
	std::list<char*> _childs;
};

#endif
