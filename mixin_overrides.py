# coding: utf-8
import abc
import json
import pickle
import sys

from sqlalchemy import Column, Integer, String, ForeignKey, MetaData, Table, \
    create_engine, Boolean
from sqlalchemy.ext.declarative import declarative_base, synonym_for, DeclarativeMeta
from sqlalchemy.orm import relationship, backref, sessionmaker, mapper

from sqlalchemy.ext.associationproxy import association_proxy


# --[ Schema and mapped classes ]------------------------------------------

class ABCDeclarativeMeta(DeclarativeMeta, abc.ABCMeta):
    pass


metadata = MetaData()
Base = declarative_base(metadata=metadata, metaclass=ABCDeclarativeMeta)

filesystem_table = Table(
    "filesystem", metadata,
    Column("fsid", Integer, primary_key=True),
    Column("device", String, nullable=False),
    Column("backend", String, nullable=False),
)


class FileSystem(object):
    def __init__(self, device, backend):
        self.device = device
        self.backend = backend


class EntryCommon(Base):
    __tablename__ = "entry"

    id = Column(Integer, primary_key=True)
    fsid = Column(Integer, ForeignKey("filesystem.fsid"), nullable=False)
    entry_type = Column("entry_type", String)
    name = Column("name", String)

    __mapper_args__ = {"polymorphic_on": entry_type}
    _filesystem = relationship(FileSystem)

    def __init__(self, name, filesystem=None):
        self.name = name
        self.filesystem = filesystem

    @property
    def filesystem(self):
        return self._filesystem

    @synonym_for("_filesystem")
    @filesystem.setter
    def filesystem(self, value):
        self._filesystem = filesystem

    def __repr__(self):
        return "<{0}: {1} ({2})>".format(self.__class__.__name__, self.name, self.id)


class Resource(Base):
    __tablename__ = "resources"

    id = Column(ForeignKey("entry.id"), primary_key=True)
    name = Column(String, primary_key=True)
    fsid = Column(Integer, ForeignKey("filesystem.fsid"), nullable=False)
    value = Column(String)

    owner = relationship(EntryCommon)
    filesystem = relationship(FileSystem)

    def __init__(self, name, value):
        self.name = name
        self.value = value


class ResourcesBearer(metaclass=abc.ABCMeta):
    """Mixin class to provide resource forks to filesystem entries."""

    @property
    @abc.abstractmethod
    def _resources(self):
        """
        Subclasses must replace this by a orm.relationship to Resource
        instances.
        """

    def __init__(self, resource_enc="json", **kwargs):
        super(ResourcesBearer, self).__init__(**kwargs)
        self.resource_enc = resource_enc

    @EntryCommon.filesystem.setter
    def filesystem(self, value):
        super(ResourcesBearer, type(self)).filesystem.__set__(self, value)
        for res in self._resources:
            res.filesystem = value

    def add_resource(self, name, value):
        encoder = self._lookup_encoder()
        self._resources.append(Resource(name=name, value=encoder.dumps(value)))

    def _lookup_encoder(self):
        return {
            "json": json,
            "pickle": pickle
        }[self.resource_enc]


mapper(FileSystem, filesystem_table)


class File(EntryCommon):
    __tablename__ = "file"
    id = Column(ForeignKey("entry.id"), primary_key=True)
    content = Column(String)

    __mapper_args__ = {"polymorphic_identity": "file"}

    def __init__(self, name, content):
        super(File, self).__init__(name=name)
        self.content = content


class Directory(ResourcesBearer, EntryCommon):
    __tablename__ = "directory"

    id = Column(ForeignKey("entry.id"), primary_key=True)
    resource_enc = Column("resource_enc", String)  # for ResourcesBearer

    __mapper_args__ = {"polymorphic_identity": "directory"}
    _resources = relationship(Resource)  # for ResourcesBearer

    entries = association_proxy("directory_entries", "entry")

    def __init__(self, name, entries=(), **kwargs):
        super(Directory, self).__init__(name=name, **kwargs)
        self.entries = entries

    @synonym_for("_filesystem")
    @EntryCommon.filesystem.setter
    def filesystem(self, value):
        super(Directory, type(self)).filesystem.__set__(self, value)
        for entry in self.entries:
            entry.filesystem = value


class DirectoryEntry(Base):
    __tablename__ = "directory_entry"

    dir_id = Column("dir_id", Integer, ForeignKey("directory.id"), primary_key=True)
    entry_id = Column("entry_id", Integer, ForeignKey("entry.id"), primary_key=True)

    directory = relationship(
        Directory,
        primaryjoin=dir_id == Directory.id,
        backref=backref("directory_entries", cascade='all,delete-orphan'),
    )

    entry = relationship(EntryCommon, foreign_keys=[entry_id])

    def __init__(self, entry):
        super(DirectoryEntry, self).__init__()
        self.entry = entry

    def __repr__(self):
        return "<DirectoryEntry @ {0:x}>".format(id(self))


class Executable(ResourcesBearer, File):
    __tablename__ = "executable"

    id = Column(ForeignKey("file.id"), primary_key=True)
    windowed = Column(Boolean)
    resource_enc = Column("resource_enc", String)  # for ResourcesBearer

    __mapper_args__ = {'polymorphic_identity': "executable"}
    _resources = relationship(Resource)  # for ResourcesBearer

    def __init__(self, name, content=None, windowed=False, **kwargs):
        super(Executable, self).__init__(name=name, content=content, **kwargs)
        self.windowed = windowed


# --[ Preparation ]------------------------------------------

database_url = sys.argv[1] if len(sys.argv) > 1 else "sqlite:///"
engine = create_engine(database_url, echo=False)
metadata.create_all(engine)

session = sessionmaker(engine)()

filesystem = FileSystem("/dev/sda", "btrfs")
session.add(filesystem)
session.commit()

documents = Directory(name="Documents")
videos = Directory(name="Videos")
videos.add_resource("Icon", {"width": 32, "height": 32, "data": "film reel"})
documents.entries = [videos, Directory(name="Pictures")]
videos.entries = [File(name="dancing.mp4", content="Cha Cha, Slow Fox and more")]
documents.filesystem = filesystem
session.add(documents)
programs = Directory(name="Programs")
cmd_exe = Executable(name="cmd.exe", resource_enc="pickle")
cmd_exe.add_resource("Icon", {"width": 32, "height": 32, "data": "binary icon"})
programs.entries = [cmd_exe]
programs.filesystem = filesystem
session.add(programs)
session.commit()

print("Content of filesystem:")
print(session.query(EntryCommon).filter_by(filesystem=filesystem).all())

print("Directories on filesystem:")
print(session.query(Directory).filter_by(filesystem=filesystem).all())

print(Directory, Directory.__mro__)
print(Executable, Executable.__mro__)
