using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Entities.Audio;
using MediaBrowser.Controller.Entities.Movies;
using MediaBrowser.Controller.Entities.TV;
using MediaBrowser.Controller.LiveTv;
using MediaBrowser.Controller.Persistence;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.Logging;
using MediaBrowser.Model.Querying;
using MediaBrowser.Model.Serialization;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MediaBrowser.Common.Extensions;
using MediaBrowser.Controller.Channels;
using MediaBrowser.Controller.Configuration;
using MediaBrowser.Controller.Playlists;
using MediaBrowser.Model.Dto;
using MediaBrowser.Model.LiveTv;

namespace MediaBrowser.Server.Implementations.Persistence
{
    /// <summary>
    /// Class SQLiteItemRepository
    /// </summary>
    public class PgsqlItemRepository : BasePgsqlRepository, IItemRepository
    {
        private IDbConnection _connection;

        private readonly TypeMapper _typeMapper = new TypeMapper();

        /// <summary>
        /// Gets the name of the repository
        /// </summary>
        /// <value>The name.</value>
        public string Name {
            get {
                return "PostgreSQL";
            }
        }

        /// <summary>
        /// Gets the json serializer.
        /// </summary>
        /// <value>The json serializer.</value>
        private readonly IJsonSerializer _jsonSerializer;

        /// <summary>
        /// The _app paths
        /// </summary>
        private readonly IServerConfigurationManager _config;

        private readonly string _criticReviewsPath;

        private IDbCommand _deleteItemCommand;

        private IDbCommand _deletePeopleCommand;
        private IDbCommand _savePersonCommand;

        private IDbCommand _deleteChaptersCommand;
        private IDbCommand _saveChapterCommand;

        private IDbCommand _deleteStreamsCommand;
        private IDbCommand _saveStreamCommand;

        private IDbCommand _deleteAncestorsCommand;
        private IDbCommand _saveAncestorCommand;

        private IDbCommand _deleteUserDataKeysCommand;
        private IDbCommand _saveUserDataKeysCommand;

        private IDbCommand _deleteItemValuesCommand;
        private IDbCommand _saveItemValuesCommand;

        private IDbCommand _deleteProviderIdsCommand;
        private IDbCommand _saveProviderIdsCommand;

        private IDbCommand _deleteImagesCommand;
        private IDbCommand _saveImagesCommand;

        private IDbCommand _updateInheritedTagsCommand;

        public const int LatestSchemaVersion = 109;

        private Table _typedBaseItems;
        private Column _guid;
        private Column _type;
        private Column _data;
        private Column _path;
        private Column _startDate;
        private Column _endDate;
        private Column _channelId;
        private Column _isKids;
        private Column _isMovie;
        private Column _isSports;
        private Column _isSeries;
        private Column _isLive;
        private Column _isNews;
        private Column _isPremiere;
        private Column _episodeTitle;
        private Column _isRepeat;
        private Column _communityRating;
        private Column _customRating;
        private Column _indexNumber;
        private Column _isLocked;
        private Column _name;
        private Column _officialRating;
        private Column _mediaType;
        private Column _overview;
        private Column _parentIndexNumber;
        private Column _premiereDate;
        private Column _productionYear;
        private Column _parentId;
        private Column _genres;
        private Column _inheritedParentalRatingValue;
        private Column _schemaVersion;
        private Column _sortName;
        private Column _runtimeTicks;
        private Column _officialRatingDescription;
        private Column _homePageUrl;
        private Column _voteCount;
        private Column _displayMediaType;
        private Column _dateCreated;
        private Column _dateModified;
        private Column _forcedSortName;
        private Column _isOffline;
        private Column _locationType;
        private Column _preferredMetadataLanguage;
        private Column _preferredMetadataCountryCode;
        private Column _isHD;
        private Column _externalEtag;
        private Column _dateLastRefreshed;
        private Column _dateLastSaved;
        private Column _isInMixedFolder;
        private Column _lockedFields;
        private Column _studios;
        private Column _audio;
        private Column _externalServiceId;
        private Column _tags;
        private Column _isFolder;
        private Column _unratedType;
        private Column _topParentId;
        private Column _isItemByName;
        private Column _sourceType;
        private Column _trailerTypes;
        private Column _criticRating;
        private Column _criticRatingSummary;
        private Column _inheritedTags;
        private Column _cleanName;
        private Column _presentationUniqueKey;
        private Column _slugName;
        private Column _originalTitle;
        private Column _primaryVersionId;
        private Column _dateLastMediaAdded;
        private Column _album;
        private Column _isVirtualItem;
        private Column _seriesName;
        private Column _userDataKey;
        private Column _seasonName;
        private Column _seasonId;
        private Column _seriesId;
        private Column _seriesSortName;
        private Index _idxTypedBaseItems1;
        private Index _idxTypedBaseItems2;
        private Index _idxTypedBaseItems3;
        private Index _idxTypedBaseItems4;
        private Index _idxTypedBaseItems5;
        private Index _idxTypedBaseItems6;
        private Index _idxTypedBaseItems7;
        private Index _idxTypedBaseItems8;
        private Index _idxTypedBaseItems9;
        private Index _idxTypedBaseItems10;
        private Index _idxTypedBaseItems11;
        private Index _idxTypedBaseItems12;
        private Index _idxTypedBaseItems13;

        private Table _ancestorIds;
        private Column _itemId;
        private Column _ancestorId;
        private Column _ancestorIdText;
        private Index _idxAncestorIds1;
        private Index _idxAncestorIds2;

        private Table _userDataKeys;
        //private Column _itemId;
        //private Column _userDataKey;
        private Column _priority;
        private Index _idxUserDataKeys;

        private Table _itemValues;
        //private Column _itemId;
        private Column _itemType;
        private Column _itemValue;
        private Column _itemCleanValue;
        private Index _idxItemValues1;
        private Index _idxItemValues2;

        private Table _providerIds;
        //private Column _itemId;
        private Column _providerName;
        private Column _providerValue;
        private Index _idxProviderIds;

        private Table _images;
        //private Column _itemId;
        private Column _imagePath;
        private Column _imageType;
        //private Column _dateModified;
        private Column _isPlaceHolder;
        private Column _sortOrder;
        private Index _idxImages;

        private Table _people;
        //private Column _itemId;
        private Column _personName;
        private Column _personRole;
        private Column _personType;
        //private Column _sortOder;
        private Column _listOrder;
        private Index _idxPeople1;
        private Index _idxPeople2;

        private Table _chapters;
        //private Column _itemId;
        private Column _chapterIndex;
        private Column _chapterStartPositionTicks;
        private Column _chapterName;
        private Column _chapterImagePath;
        private Column _chapterImageDateModified;

        private Table _mediaStreams;
        private Column _streamIndex;
        private Column _streamType;
        private Column _codec;
        private Column _language;
        private Column _channelLayout;
        private Column _profile;
        private Column _aspectRatio;
        //private Column _path;
        private Column _isInterlaced;
        private Column _bitRate;
        private Column _channels;
        private Column _sampleRate;
        private Column _isDefault;
        private Column _isForced;
        private Column _isExternal;
        private Column _height;
        private Column _width;
        private Column _averageFrameRate;
        private Column _realFrameRate;
        private Column _level;
        private Column _pixelFormat;
        private Column _bitDepth;
        private Column _isAnamorphic;
        private Column _refFrames;
        private Column _keyFrames;
        private Column _codecTag;
        private Column _comment;
        private Column _nalLength;
        private Column _isAVC;
        private Column _title;
        private Column _timeBase;
        private Column _codecTimeBase;
        private Index _idxMediaStreams;

        /// <summary>
        /// Initializes a new instance of the <see cref="SqliteItemRepository"/> class.
        /// </summary>
        public PgsqlItemRepository(IServerConfigurationManager config, IJsonSerializer jsonSerializer, ILogManager logManager, IDbConnector connector)
            : base(logManager, connector)
        {
            if (config == null)
                {
                    throw new ArgumentNullException("config");
                }
            if (jsonSerializer == null)
                {
                    throw new ArgumentNullException("jsonSerializer");
                }

            _config = config;
            _jsonSerializer = jsonSerializer;

            _criticReviewsPath = Path.Combine(_config.ApplicationPaths.DataPath, "critic-reviews");

            host = "localhost";
            port = 5432;
            username = "emby";
            password = "emby";
            database = "emby";

            _guid = new Column("Guid", ColumnType.UniqueIdentifier);
            _type = new Column("Type", ColumnType.Text);
            _data = new Column("Data", ColumnType.Binary);
            _path = new Column("Path", ColumnType.Text);
            _startDate = new Column("StartDate", ColumnType.Date);
            _endDate = new Column("EndDate", ColumnType.Date);
            _channelId = new Column("ChannelId", ColumnType.Text);
            _isKids = new Column("IsKids", ColumnType.Boolean);
            _isMovie = new Column("IsMovie", ColumnType.Boolean);
            _isSports = new Column("IsSports", ColumnType.Boolean);
            _isSeries = new Column("IsSeries", ColumnType.Boolean);
            _isLive = new Column("IsLive", ColumnType.Boolean);
            _isNews = new Column("IsNews", ColumnType.Boolean);
            _isPremiere = new Column("IsPremiere", ColumnType.Boolean);
            _episodeTitle = new Column("EpisodeTitle", ColumnType.Text);
            _isRepeat = new Column("IsRepeat", ColumnType.Boolean);
            _communityRating = new Column("CommunityRating", ColumnType.Real);
            _customRating = new Column("CustomRating", ColumnType.Text);
            _indexNumber = new Column("IndexNumber", ColumnType.Integer);
            _isLocked = new Column("IsLocked", ColumnType.Boolean);
            _name = new Column("Name", ColumnType.Text);
            _officialRating = new Column("OfficialRating", ColumnType.Text);
            _mediaType = new Column("MediaType", ColumnType.Text);
            _overview = new Column("Overview", ColumnType.Text);
            _parentIndexNumber = new Column("ParentIndexNumber", ColumnType.Integer);
            _premiereDate = new Column("PremiereDate", ColumnType.Date);
            _productionYear = new Column("ProductionYear", ColumnType.Integer);
            _parentId = new Column("ParentId", ColumnType.UniqueIdentifier);
            _genres = new Column("Genres", ColumnType.Text);
            _inheritedParentalRatingValue = new Column("InheritedParentalRatingValue", ColumnType.Integer);
            _schemaVersion = new Column("SchemaVersion", ColumnType.Integer);
            _sortName = new Column("SortName", ColumnType.Text);
            _runtimeTicks = new Column("RuntimeTicks", ColumnType.BigInteger);
            _officialRatingDescription = new Column("OfficialRatingDescription", ColumnType.Text);
            _homePageUrl = new Column("HomePageUrl", ColumnType.Text);
            _voteCount = new Column("VoteCount", ColumnType.Integer);
            _displayMediaType = new Column("DisplayMediaType", ColumnType.Text);
            _dateCreated = new Column("DateCreated", ColumnType.Date);
            _dateModified = new Column("DateModified", ColumnType.Date);
            _forcedSortName = new Column("ForcedSortName", ColumnType.Text);
            _isOffline = new Column("IsOffline", ColumnType.Boolean);
            _locationType = new Column("LocationType", ColumnType.Text);
            _preferredMetadataLanguage = new Column("PreferredMetadataLanguage", ColumnType.Text);
            _preferredMetadataCountryCode = new Column("PreferredMetadataCountryCode", ColumnType.Text);
            _isHD = new Column("IsHD", ColumnType.Boolean);
            _externalEtag = new Column("ExternalEtag", ColumnType.Text);
            _dateLastRefreshed = new Column("DateLastRefreshed", ColumnType.Date);
            _dateLastSaved = new Column("DateLastSaved", ColumnType.Date);
            _isInMixedFolder = new Column("IsInMixedFolder", ColumnType.Boolean);
            _lockedFields = new Column("LockedFields", ColumnType.Text);
            _studios = new Column("Studios", ColumnType.Text);
            _audio = new Column("Audio", ColumnType.Text);
            _externalServiceId = new Column("ExternalServiceId", ColumnType.Text);
            _tags = new Column("Tags", ColumnType.Text);
            _isFolder = new Column("IsFolder", ColumnType.Boolean);
            _unratedType = new Column("UnratedType", ColumnType.Text);
            _topParentId = new Column("TopParentId", ColumnType.Text);
            _isItemByName = new Column("IsItemByName", ColumnType.Boolean);
            _sourceType = new Column("SourceType", ColumnType.Text);
            _trailerTypes = new Column("TrailerTypes", ColumnType.Text);
            _criticRating = new Column("CriticRating", ColumnType.Real);
            _criticRatingSummary = new Column("CriticRatingSummary", ColumnType.Text);
            _inheritedTags = new Column("InheritedTags", ColumnType.Text);
            _cleanName = new Column("CleanName", ColumnType.Text);
            _presentationUniqueKey = new Column("PresentationUniqueKey", ColumnType.Text);
            _slugName = new Column("SlugName", ColumnType.Text);
            _originalTitle = new Column("OriginalTitle", ColumnType.Text);
            _primaryVersionId = new Column("PrimaryVersionId", ColumnType.Text);
            _dateLastMediaAdded = new Column("DateLastMediaAdded", ColumnType.Date);
            _album = new Column("Album", ColumnType.Text);
            _isVirtualItem = new Column("IsVirtualItem", ColumnType.Boolean);
            _seriesName = new Column("SeriesName", ColumnType.Text);
            _userDataKey = new Column("UserDataKey", ColumnType.Text);
            _seasonName = new Column("SeasonName", ColumnType.Text);
            _seasonId = new Column("SeasonId", ColumnType.UniqueIdentifier);
            _seriesId = new Column("SeriesId", ColumnType.UniqueIdentifier);
            _seriesSortName = new Column("SeriesSortName", ColumnType.Text);

            _typedBaseItems = new Table("TypedBaseItems",
                new Column[] {
                    _guid
                },
                new Column[] {
                    _type,
                    _data,
                    _path,
                    _startDate,
                    _endDate,
                    _channelId,
                    _isKids,
                    _isMovie,
                    _isSports,
                    _isSeries,
                    _isLive,
                    _isNews,
                    _isPremiere,
                    _episodeTitle,
                    _isRepeat,
                    _communityRating,
                    _customRating,
                    _indexNumber,
                    _isLocked,
                    _name,
                    _officialRating,
                    _mediaType,
                    _overview,
                    _parentIndexNumber,
                    _premiereDate,
                    _productionYear,
                    _parentId,
                    _genres,
                    _inheritedParentalRatingValue,
                    _schemaVersion,
                    _sortName,
                    _runtimeTicks,
                    _officialRatingDescription,
                    _homePageUrl,
                    _voteCount,
                    _displayMediaType,
                    _dateCreated,
                    _dateModified,
                    _forcedSortName,
                    _isOffline,
                    _locationType,
                    _preferredMetadataLanguage,
                    _preferredMetadataCountryCode,
                    _isHD,
                    _externalEtag,
                    _dateLastRefreshed,
                    _dateLastSaved,
                    _isInMixedFolder,
                    _lockedFields,
                    _studios,
                    _audio,
                    _externalServiceId,
                    _tags,
                    _isFolder,
                    _unratedType,
                    _topParentId,
                    _isItemByName,
                    _sourceType,
                    _trailerTypes,
                    _criticRating,
                    _criticRatingSummary,
                    _inheritedTags,
                    _cleanName,
                    _presentationUniqueKey,
                    _slugName,
                    _originalTitle,
                    _primaryVersionId,
                    _dateLastMediaAdded,
                    _album,
                    _isVirtualItem,
                    _seriesName,
                    _userDataKey,
                    _seasonName,
                    _seasonId,
                    _seriesId,
                    _seriesSortName
                });

            _idxTypedBaseItems1 = new Index("typedbaseitems_path", _typedBaseItems, new Column[] { _path });
            _idxTypedBaseItems2 = new Index("typedbaseitems_parentid", _typedBaseItems, new Column[] { _parentId });
            _idxTypedBaseItems3 = new Index("typedbaseitems_presentationuniquekey", _typedBaseItems, new Column[] { _presentationUniqueKey });
            _idxTypedBaseItems4 = new Index("typedbaseitems_guid_type_isfolder_isvirtualitem", _typedBaseItems, new Column[] {
                _guid,
                _type,
                _isFolder,
                _isVirtualItem
            });
            _idxTypedBaseItems5 = new Index("typedbaseitems_cleanname_type", _typedBaseItems, new Column[] { 
                _cleanName,
                _type
            });
            // covering index
            _idxTypedBaseItems6 = new Index("typedbaseitems_topparentid_guid", _typedBaseItems, new Column[] { 
                _topParentId,
                _guid
            });
            // live tv programs
            _idxTypedBaseItems7 = new Index("typedbaseitems_type_topparentid_startdate", _typedBaseItems, new Column[] { 
                _type,
                _topParentId,
                _startDate
            });
            // covering index for getitemvalues
            _idxTypedBaseItems8 = new Index("typedbaseitems_type_topparentid_guid", _typedBaseItems, new Column[] { 
                _type,
                _topParentId,
                _guid
            });
            // used by movie suggestions
            _idxTypedBaseItems9 = new Index("typedbaseitems_type_topparentid_presentationuniquekey", _typedBaseItems, new Column[] { 
                _type,
                _topParentId,
                _presentationUniqueKey
            });
            _idxTypedBaseItems10 = new Index("typedbaseitems_topparentid_isVirtualItem", _typedBaseItems, new Column[] { 
                _topParentId,
                _isVirtualItem
            });
            // latest items
            _idxTypedBaseItems11 = new Index("typedbaseitems_topparentid_type_presentationuniquekey_datecreated", _typedBaseItems, new Column[] { 
                _topParentId,
                _type,
                _presentationUniqueKey,
                _dateCreated
            });
            _idxTypedBaseItems12 = new Index("typedbaseitems_topparentid_isfolder_isvirtualitem_presentationuniquekey_datecreated", _typedBaseItems, new Column[] { 
                _topParentId,
                _isFolder,
                _isVirtualItem,
                _presentationUniqueKey,
                _dateCreated
            });
            // resume
            _idxTypedBaseItems13 = new Index("typedbaseitems_topparentid_mediatype_isvirtualitem_presentationuniquekey", _typedBaseItems, new Column[] { 
                _topParentId,
                _mediaType,
                _isVirtualItem,
                _presentationUniqueKey
            });

            _itemId = new Column("ItemId", ColumnType.UniqueIdentifier, true);
            _ancestorId = new Column("AncestorId", ColumnType.UniqueIdentifier);
            _ancestorIdText = new Column("AncestorIdText", ColumnType.Text);

            _ancestorIds = new Table("AncestorIds",
                new Column[] {
                    _itemId,
                    _ancestorId
                },
                new Column[] {
                    _ancestorIdText
                });

            _idxAncestorIds1 = new Index("ancestorids_ancestorid", _ancestorIds, new Column[] { _ancestorId });
            _idxAncestorIds2 = new Index("ancestorids_ancestoridtext", _ancestorIds, new Column[] { _ancestorIdText });

            _priority = new Column("Priority", ColumnType.Integer);

            _userDataKeys = new Table("UserDataKeys",
                new Column[] {
                    _itemId,
                    _userDataKey
                },
                new Column[] {
                    _priority
                });

            // covering index
            _idxUserDataKeys = new Index("userdatakeys_itemid_priority_userdatakey", _userDataKeys, new Column[] {
                _itemId,
                _priority,
                _userDataKey
            });

            _itemType = new Column("Type", ColumnType.Integer);
            _itemValue = new Column("Value", ColumnType.Text);
            _itemCleanValue = new Column("CleanValue", ColumnType.Text);
            
            _itemValues = new Table("ItemValues",
                new Column[] { },
                new Column[] {
                    _itemId,
                    _itemType,
                    _itemValue,
                    _itemCleanValue
                });

            // items by name
            _idxItemValues1 = new Index("itemvalues_itemid_type_cleanvalue", _itemValues, new Column[] {
                _itemId,
                _itemType,
                _itemCleanValue
            });
            _idxItemValues2 = new Index("itemvalues_type_cleanvalue_itemid", _itemValues, new Column[] {
                _itemType,
                _itemCleanValue,
                _itemId
            });

            _providerName = new Column("Name", ColumnType.Text);
            _providerValue = new Column("Value", ColumnType.Text);

            _providerIds = new Table("ProviderIds",
                new Column[] {
                    _itemId,
                    _providerName
                },
                new Column[] {
                    _providerValue
                });

            _idxProviderIds = new Index("providerids_itemid_name_value", _providerIds, new Column[] {
                _itemId,
                _providerName,
                _providerValue
            });


            _imagePath = new Column("Path", ColumnType.Text, true);
            _imageType = new Column("Type", ColumnType.Integer, true);
            _isPlaceHolder = new Column("IsPlaceHolder", ColumnType.Boolean);
            _sortOrder = new Column("SortOrder", ColumnType.Integer);

            _images = new Table("Images", 
                new Column[] { },
                new Column[] {
                    _itemId,
                    _imagePath,
                    _imageType,
                    _dateModified,
                    _isPlaceHolder,
                    _sortOrder
                });

            _idxImages = new Index("images_itemid", _images, new Column[] { _itemId });

            _personName = new Column("Name", ColumnType.Text, true);
            _personRole = new Column("Role", ColumnType.Text);
            _personType = new Column("Type", ColumnType.Text);
            _listOrder = new Column("ListOrder", ColumnType.Integer);

            _people = new Table("People",
                new Column[] { },
                new Column[] {
                    _itemId,
                    _personName,
                    _personRole,
                    _personType,
                    _sortOrder,
                    _listOrder
                });

            _idxPeople1 = new Index("people_itemid_listorder", _people, new Column[] {
                _itemId,
                _listOrder
            });

            _idxPeople2 = new Index("people_name", _people, new Column[] { _personName });

            _chapterIndex = new Column("Index", ColumnType.Integer);
            _chapterStartPositionTicks = new Column("StartPositionTicks", ColumnType.BigInteger);
            _chapterName = new Column("Name", ColumnType.Text);
            _chapterImagePath = new Column("ImagePath", ColumnType.Text);
            _chapterImageDateModified = new Column("ImageDateModified", ColumnType.Date);

            _chapters = new Table("Chapters", 
                new Column[] {
                    _itemId,
                    _chapterIndex
                },
                new Column[] {
                    _chapterStartPositionTicks,
                    _chapterName,
                    _chapterImagePath,
                    _chapterImageDateModified
                });

            _streamIndex = new Column("Index", ColumnType.Integer);
            _streamType = new Column("Type", ColumnType.Text);
            _codec = new Column("Codec", ColumnType.Text);
            _language = new Column("Language", ColumnType.Text);
            _channelLayout = new Column("ChannelLayout", ColumnType.Text);
            _profile = new Column("Profile", ColumnType.Text);
            _aspectRatio = new Column("AspectRatio", ColumnType.Text);
            _isInterlaced = new Column("IsInterlaced", ColumnType.Boolean);
            _bitRate = new Column("BitRate", ColumnType.Integer);
            _channels = new Column("Channels", ColumnType.Integer);
            _sampleRate = new Column("SampleRate", ColumnType.Integer);
            _isDefault = new Column("IsDefault", ColumnType.Boolean);
            _isForced = new Column("IsForced", ColumnType.Boolean);
            _isExternal = new Column("IsExternal", ColumnType.Boolean);
            _height = new Column("Height", ColumnType.Integer);
            _width = new Column("Width", ColumnType.Integer);
            _averageFrameRate = new Column("AverageFrameRate", ColumnType.Real);
            _realFrameRate = new Column("RealFrameRate", ColumnType.Real);
            _level = new Column("Level", ColumnType.Real);
            _pixelFormat = new Column("PixelFormat", ColumnType.Text);
            _bitDepth = new Column("BitDepth", ColumnType.Integer);
            _isAnamorphic = new Column("IsAnamorphic", ColumnType.Boolean);
            _refFrames = new Column("RefFrames", ColumnType.Integer);
            _keyFrames = new Column("KeyFrames", ColumnType.Text);
            _codecTag = new Column("CodecTag", ColumnType.Text);
            _comment = new Column("Comment", ColumnType.Text);
            _nalLength = new Column("NalLength", ColumnType.Text);
            _isAVC = new Column("IsAVC", ColumnType.Boolean);
            _title = new Column("Title", ColumnType.Text);
            _timeBase = new Column("TimeBase", ColumnType.Text);
            _codecTimeBase = new Column("CodecTimeBase", ColumnType.Text);

            _mediaStreams = new Table("MediaStreams", 
                new Column[] {
                    _itemId,
                    _streamIndex
                },
                new Column[] {
                    _streamType,
                    _codec,
                    _language,
                    _channelLayout,
                    _profile,
                    _aspectRatio,
                    _path,
                    _isInterlaced,
                    _bitRate,
                    _channels,
                    _sampleRate,
                    _isDefault,
                    _isForced,
                    _isExternal,
                    _height,
                    _width,
                    _averageFrameRate,
                    _realFrameRate,
                    _level,
                    _pixelFormat,
                    _bitDepth,
                    _isAnamorphic,
                    _refFrames,
                    _keyFrames,
                    _codecTag,
                    _comment,
                    _nalLength,
                    _isAVC,
                    _title,
                    _timeBase,
                    _codecTimeBase
                });

            _idxMediaStreams = new Index("mediastreams_itemid", _mediaStreams, new Column[] { _itemId });
        }

        private const string ChaptersTableName = "Chapters2";

        /// <summary>
        /// Opens the connection to the database
        /// </summary>
        /// <returns>Task.</returns>
        public async Task Initialize(IUserDataRepository userDataRepo)
        {
            _connection = await CreateConnection().ConfigureAwait(false);

            string[] queries = {
                _typedBaseItems.CreateQuery,
                _idxTypedBaseItems1.CreateQuery,
                _idxTypedBaseItems2.CreateQuery,
                _idxTypedBaseItems3.CreateQuery,
                _idxTypedBaseItems4.CreateQuery,
                _idxTypedBaseItems5.CreateQuery,
                _idxTypedBaseItems6.CreateQuery,
                _idxTypedBaseItems7.CreateQuery,
                _idxTypedBaseItems8.CreateQuery,
                _idxTypedBaseItems9.CreateQuery,
                _idxTypedBaseItems10.CreateQuery,
                _idxTypedBaseItems11.CreateQuery,
                _idxTypedBaseItems12.CreateQuery,
                _idxTypedBaseItems13.CreateQuery,
                _ancestorIds.CreateQuery,
                _idxAncestorIds1.CreateQuery,
                _idxAncestorIds2.CreateQuery,
                _userDataKeys.CreateQuery,
                _itemValues.CreateQuery,
                _idxItemValues1.CreateQuery,
                _idxItemValues2.CreateQuery,
                _providerIds.CreateQuery,
                _idxProviderIds.CreateQuery,
                _images.CreateQuery,
                _idxImages.CreateQuery,
                _people.CreateQuery,
                _idxPeople1.CreateQuery,
                _idxPeople2.CreateQuery,
                _chapters.CreateQuery,
                _mediaStreams.CreateQuery,
                _idxMediaStreams.CreateQuery
            };

            _connection.RunQueries(queries, Logger);

            PrepareStatements();

            await userDataRepo.Initialize(_connection, WriteLock).ConfigureAwait(false);
            //await Vacuum(_connection).ConfigureAwait(false);
        }

        private readonly string[] _retriveItemColumns = {
            "type",
            "data",
            "StartDate",
            "EndDate",
            "IsOffline",
            "ChannelId",
            "IsMovie",
            "IsSports",
            "IsKids",
            "IsSeries",
            "IsLive",
            "IsNews",
            "IsPremiere",
            "EpisodeTitle",
            "IsRepeat",
            "CommunityRating",
            "CustomRating",
            "IndexNumber",
            "IsLocked",
            "PreferredMetadataLanguage",
            "PreferredMetadataCountryCode",
            "IsHD",
            "ExternalEtag",
            "DateLastRefreshed",
            "Name",
            "Path",
            "PremiereDate",
            "Overview",
            "ParentIndexNumber",
            "ProductionYear",
            "OfficialRating",
            "OfficialRatingDescription",
            "HomePageUrl",
            "DisplayMediaType",
            "ForcedSortName",
            "RunTimeTicks",
            "VoteCount",
            "DateCreated",
            "DateModified",
            "guid",
            "Genres",
            "ParentId",
            "Audio",
            "ExternalServiceId",
            "IsInMixedFolder",
            "DateLastSaved",
            "LockedFields",
            "Studios",
            "Tags",
            "SourceType",
            "TrailerTypes",
            "OriginalTitle",
            "PrimaryVersionId",
            "DateLastMediaAdded",
            "Album",
            "CriticRating",
            "CriticRatingSummary",
            "IsVirtualItem",
            "SeriesName",
            "SeasonName",
            "SeasonId",
            "SeriesId",
            "SeriesSortName",
            "PresentationUniqueKey",
            "InheritedParentalRatingValue"
        };

        private readonly string[] _mediaStreamSaveColumns = {
            "ItemId",
            "StreamIndex",
            "StreamType",
            "Codec",
            "Language",
            "ChannelLayout",
            "Profile",
            "AspectRatio",
            "Path",
            "IsInterlaced",
            "BitRate",
            "Channels",
            "SampleRate",
            "IsDefault",
            "IsForced",
            "IsExternal",
            "Height",
            "Width",
            "AverageFrameRate",
            "RealFrameRate",
            "Level",
            "PixelFormat",
            "BitDepth",
            "IsAnamorphic",
            "RefFrames",
            "CodecTag",
            "Comment",
            "NalLengthSize",
            "IsAvc",
            "Title",
            "TimeBase",
            "CodecTimeBase"
        };

        /// <summary>
        /// Prepares the statements.
        /// </summary>
        private void PrepareStatements()
        {
            _deleteItemCommand = _connection.CreateCommand();
            _deleteItemCommand.CommandText = "DELETE FROM TypedBaseItems WHERE guid = @Id";
            _deleteItemCommand.Parameters.Add(_deleteItemCommand, "@Id");

            // People
            _deletePeopleCommand = _connection.CreateCommand();
            _deletePeopleCommand.CommandText = "DELETE FROM People WHERE ItemId = @Id";
            _deletePeopleCommand.Parameters.Add(_deletePeopleCommand, "@Id");

            _savePersonCommand = _connection.CreateCommand();
            _savePersonCommand.CommandText = "INSERT INTO People (ItemId, Name, Role, PersonType, SortOrder, ListOrder) VALUES (@ItemId, @Name, @Role, @PersonType, @SortOrder, @ListOrder)";
            _savePersonCommand.Parameters.Add(_savePersonCommand, "@ItemId");
            _savePersonCommand.Parameters.Add(_savePersonCommand, "@Name");
            _savePersonCommand.Parameters.Add(_savePersonCommand, "@Role");
            _savePersonCommand.Parameters.Add(_savePersonCommand, "@PersonType");
            _savePersonCommand.Parameters.Add(_savePersonCommand, "@SortOrder");
            _savePersonCommand.Parameters.Add(_savePersonCommand, "@ListOrder");

            // Ancestors
            _deleteAncestorsCommand = _connection.CreateCommand();
            _deleteAncestorsCommand.CommandText = "DELETE FROM AncestorIds WHERE ItemId = @Id";
            _deleteAncestorsCommand.Parameters.Add(_deleteAncestorsCommand, "@Id");

            _saveAncestorCommand = _connection.CreateCommand();
            _saveAncestorCommand.CommandText = "INSERT INTO AncestorIds (ItemId, AncestorId, AncestorIdText) VALUES (@ItemId, @AncestorId, @AncestorIdText)";
            _saveAncestorCommand.Parameters.Add(_saveAncestorCommand, "@ItemId");
            _saveAncestorCommand.Parameters.Add(_saveAncestorCommand, "@AncestorId");
            _saveAncestorCommand.Parameters.Add(_saveAncestorCommand, "@AncestorIdText");

            // Chapters
            _deleteChaptersCommand = _connection.CreateCommand();
            _deleteChaptersCommand.CommandText = "DELETE FROM " + ChaptersTableName + " WHERE ItemId = @ItemId";
            _deleteChaptersCommand.Parameters.Add(_deleteChaptersCommand, "@ItemId");

            _saveChapterCommand = _connection.CreateCommand();
            _saveChapterCommand.CommandText = "INSERT INTO " + ChaptersTableName + " (ItemId, ChapterIndex, StartPositionTicks, Name, ImagePath) VALUES (@ItemId, @ChapterIndex, @StartPositionTicks, @Name, @ImagePath) ON CONFLICT (ItemId, ChapterIndex) DO UPDATE SET StartPositionTicks = @StartPositionTicks, Name = @Name, ImagePath = @ImagePath";

            _saveChapterCommand.Parameters.Add(_saveChapterCommand, "@ItemId");
            _saveChapterCommand.Parameters.Add(_saveChapterCommand, "@ChapterIndex");
            _saveChapterCommand.Parameters.Add(_saveChapterCommand, "@StartPositionTicks");
            _saveChapterCommand.Parameters.Add(_saveChapterCommand, "@Name");
            _saveChapterCommand.Parameters.Add(_saveChapterCommand, "@ImagePath");
            _saveChapterCommand.Parameters.Add(_saveChapterCommand, "@ImageDateModified");

            // MediaStreams
            _deleteStreamsCommand = _connection.CreateCommand();
            _deleteStreamsCommand.CommandText = "DELETE FROM mediastreams WHERE ItemId = @ItemId";
            _deleteStreamsCommand.Parameters.Add(_deleteStreamsCommand, "@ItemId");

            _saveStreamCommand = _connection.CreateCommand();

            _saveStreamCommand.CommandText = string.Format("INSERT INTO mediastreams ({0}) VALUES ({1}) ON CONFLICT (ItemId, StreamIndex) DO UPDATE SET ",
                string.Join(",", _mediaStreamSaveColumns),
                string.Join(",", _mediaStreamSaveColumns.Select(i => "@" + i).ToArray()));
            
            for (var i = 2; i < _mediaStreamSaveColumns.Length; i++)
                {
                    string col = _mediaStreamSaveColumns[i];
                    if (i > 2)
                        {
                            _saveStreamCommand.CommandText += ", ";
                        }
                    _saveStreamCommand.CommandText += col + " = @" + col;
                }

            foreach (var col in _mediaStreamSaveColumns)
                {
                    _saveStreamCommand.Parameters.Add(_saveStreamCommand, "@" + col);
                }

            _updateInheritedTagsCommand = _connection.CreateCommand();
            _updateInheritedTagsCommand.CommandText = "UPDATE TypedBaseItems SET InheritedTags = @InheritedTags where guid = @Guid";
            _updateInheritedTagsCommand.Parameters.Add(_updateInheritedTagsCommand, "@Guid");
            _updateInheritedTagsCommand.Parameters.Add(_updateInheritedTagsCommand, "@InheritedTags");

            // user data
            _deleteUserDataKeysCommand = _connection.CreateCommand();
            _deleteUserDataKeysCommand.CommandText = "DELETE FROM UserDataKeys where ItemId = @ItemId";
            _deleteUserDataKeysCommand.Parameters.Add(_deleteUserDataKeysCommand, "@ItemId");

            _saveUserDataKeysCommand = _connection.CreateCommand();
            _saveUserDataKeysCommand.CommandText = "INSERT INTO UserDataKeys (ItemId, UserDataKey, Priority) VALUES (@ItemId, @UserDataKey, @Priority)";
            _saveUserDataKeysCommand.Parameters.Add(_saveUserDataKeysCommand, "@ItemId");
            _saveUserDataKeysCommand.Parameters.Add(_saveUserDataKeysCommand, "@UserDataKey");
            _saveUserDataKeysCommand.Parameters.Add(_saveUserDataKeysCommand, "@Priority");

            // item values
            _deleteItemValuesCommand = _connection.CreateCommand();
            _deleteItemValuesCommand.CommandText = "DELETE FROM ItemValues where ItemId = @Id";
            _deleteItemValuesCommand.Parameters.Add(_deleteItemValuesCommand, "@ItemId");

            _saveItemValuesCommand = _connection.CreateCommand();
            _saveItemValuesCommand.CommandText = "INSERT INTO ItemValues (ItemId, Type, Value, CleanValue) VALUES (@ItemId, @Type, @Value, @CleanValue)";
            _saveItemValuesCommand.Parameters.Add(_saveItemValuesCommand, "@ItemId");
            _saveItemValuesCommand.Parameters.Add(_saveItemValuesCommand, "@Type");
            _saveItemValuesCommand.Parameters.Add(_saveItemValuesCommand, "@Value");
            _saveItemValuesCommand.Parameters.Add(_saveItemValuesCommand, "@CleanValue");

            // provider ids
            _deleteProviderIdsCommand = _connection.CreateCommand();
            _deleteProviderIdsCommand.CommandText = "DELETE FROM ProviderIds WHERE ItemId = @ItemId";
            _deleteProviderIdsCommand.Parameters.Add(_deleteProviderIdsCommand, "@ItemId");

            _saveProviderIdsCommand = _connection.CreateCommand();
            _saveProviderIdsCommand.CommandText = "INSERT INTO ProviderIds (ItemId, Name, Value) VALUES (@ItemId, @Name, @Value)";
            _saveProviderIdsCommand.Parameters.Add(_saveProviderIdsCommand, "@ItemId");
            _saveProviderIdsCommand.Parameters.Add(_saveProviderIdsCommand, "@Name");
            _saveProviderIdsCommand.Parameters.Add(_saveProviderIdsCommand, "@Value");

            // images
            _deleteImagesCommand = _connection.CreateCommand();
            _deleteImagesCommand.CommandText = "DELETE FROM Images WHERE ItemId = @ItemId";
            _deleteImagesCommand.Parameters.Add(_deleteImagesCommand, "@ItemId");

            _saveImagesCommand = _connection.CreateCommand();
            _saveImagesCommand.CommandText = "INSERT INTO Images (ItemId, ImageType, Path, DateModified, IsPlaceHolder, SortOrder) VALUES (@ItemId, @ImageType, @Path, @DateModified, @IsPlaceHolder, @SortOrder)";
            _saveImagesCommand.Parameters.Add(_saveImagesCommand, "@ItemId");
            _saveImagesCommand.Parameters.Add(_saveImagesCommand, "@ImageType");
            _saveImagesCommand.Parameters.Add(_saveImagesCommand, "@Path");
            _saveImagesCommand.Parameters.Add(_saveImagesCommand, "@DateModified");
            _saveImagesCommand.Parameters.Add(_saveImagesCommand, "@IsPlaceHolder");
            _saveImagesCommand.Parameters.Add(_saveImagesCommand, "@SortOrder");
        }

        /// <summary>
        /// Save a standard item in the repo
        /// </summary>
        /// <param name="item">The item.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>Task.</returns>
        /// <exception cref="System.ArgumentNullException">item</exception>
        public Task SaveItem(BaseItem item, CancellationToken cancellationToken)
        {
            if (item == null)
                {
                    throw new ArgumentNullException("item");
                }

            return SaveItems(new[] { item }, cancellationToken);
        }

        /// <summary>
        /// Saves the items.
        /// </summary>
        /// <param name="items">The items.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>Task.</returns>
        /// <exception cref="System.ArgumentNullException">
        /// items
        /// or
        /// cancellationToken
        /// </exception>
        public async Task SaveItems(IEnumerable<BaseItem> items, CancellationToken cancellationToken)
        {
            if (items == null)
                {
                    throw new ArgumentNullException("items");
                }

            cancellationToken.ThrowIfCancellationRequested();

            CheckDisposed();

            await WriteLock.WaitAsync(cancellationToken).ConfigureAwait(false);

            IDbTransaction transaction = null;

            try
                {
                    transaction = _connection.BeginTransaction();

                    using (var cmd = _typedBaseItems.InsertCommand)
                        {
                            cmd.Connection = _connection;

                            foreach (var item in items)
                                {
                                    cancellationToken.ThrowIfCancellationRequested();

                                    var index = 0;

                                    cmd.GetParameter(index++).Value = item.Id;
                                    cmd.GetParameter(index++).Value = item.GetType().FullName;
                                    cmd.GetParameter(index++).Value = _jsonSerializer.SerializeToBytes(item);

                                    cmd.GetParameter(index++).Value = item.Path;

                                    var hasStartDate = item as IHasStartDate;
                                    if (hasStartDate != null)
                                        {
                                            cmd.GetParameter(index++).Value = hasStartDate.StartDate;
                                        } else
                                        {
                                            cmd.GetParameter(index++).Value = null;
                                        }

                                    cmd.GetParameter(index++).Value = item.EndDate;
                                    cmd.GetParameter(index++).Value = item.ChannelId;

                                    var hasProgramAttributes = item as IHasProgramAttributes;
                                    if (hasProgramAttributes != null)
                                        {
                                            cmd.GetParameter(index++).Value = hasProgramAttributes.IsKids;
                                            cmd.GetParameter(index++).Value = hasProgramAttributes.IsMovie;
                                            cmd.GetParameter(index++).Value = hasProgramAttributes.IsSports;
                                            cmd.GetParameter(index++).Value = hasProgramAttributes.IsSeries;
                                            cmd.GetParameter(index++).Value = hasProgramAttributes.IsLive;
                                            cmd.GetParameter(index++).Value = hasProgramAttributes.IsNews;
                                            cmd.GetParameter(index++).Value = hasProgramAttributes.IsPremiere;
                                            cmd.GetParameter(index++).Value = hasProgramAttributes.EpisodeTitle;
                                            cmd.GetParameter(index++).Value = hasProgramAttributes.IsRepeat;
                                        } else
                                        {
                                            cmd.GetParameter(index++).Value = null;
                                            cmd.GetParameter(index++).Value = null;
                                            cmd.GetParameter(index++).Value = null;
                                            cmd.GetParameter(index++).Value = null;
                                            cmd.GetParameter(index++).Value = null;
                                            cmd.GetParameter(index++).Value = null;
                                            cmd.GetParameter(index++).Value = null;
                                            cmd.GetParameter(index++).Value = null;
                                            cmd.GetParameter(index++).Value = null;
                                        }

                                    cmd.GetParameter(index++).Value = item.CommunityRating;
                                    cmd.GetParameter(index++).Value = item.CustomRating;

                                    cmd.GetParameter(index++).Value = item.IndexNumber;
                                    cmd.GetParameter(index++).Value = item.IsLocked;

                                    cmd.GetParameter(index++).Value = item.Name;
                                    cmd.GetParameter(index++).Value = item.OfficialRating;

                                    cmd.GetParameter(index++).Value = item.MediaType;
                                    cmd.GetParameter(index++).Value = item.Overview;
                                    cmd.GetParameter(index++).Value = item.ParentIndexNumber;
                                    cmd.GetParameter(index++).Value = item.PremiereDate;
                                    cmd.GetParameter(index++).Value = item.ProductionYear;

                                    if (item.ParentId == Guid.Empty)
                                        {
                                            cmd.GetParameter(index++).Value = null;
                                        } else
                                        {
                                            cmd.GetParameter(index++).Value = item.ParentId;
                                        }

                                    cmd.GetParameter(index++).Value = string.Join("|", item.Genres.ToArray());
                                    cmd.GetParameter(index++).Value = item.GetInheritedParentalRatingValue() ?? 0;

                                    cmd.GetParameter(index++).Value = LatestSchemaVersion;
                                    cmd.GetParameter(index++).Value = item.SortName;
                                    cmd.GetParameter(index++).Value = item.RunTimeTicks;

                                    cmd.GetParameter(index++).Value = item.OfficialRatingDescription;
                                    cmd.GetParameter(index++).Value = item.HomePageUrl;
                                    cmd.GetParameter(index++).Value = item.VoteCount;
                                    cmd.GetParameter(index++).Value = item.DisplayMediaType;
                                    cmd.GetParameter(index++).Value = item.DateCreated;
                                    cmd.GetParameter(index++).Value = item.DateModified;

                                    cmd.GetParameter(index++).Value = item.ForcedSortName;
                                    cmd.GetParameter(index++).Value = item.IsOffline;
                                    cmd.GetParameter(index++).Value = item.LocationType.ToString();

                                    cmd.GetParameter(index++).Value = item.PreferredMetadataLanguage;
                                    cmd.GetParameter(index++).Value = item.PreferredMetadataCountryCode;
                                    cmd.GetParameter(index++).Value = item.IsHD;
                                    cmd.GetParameter(index++).Value = item.ExternalEtag;

                                    if (item.DateLastRefreshed == default(DateTime))
                                        {
                                            cmd.GetParameter(index++).Value = null;
                                        } else
                                        {
                                            cmd.GetParameter(index++).Value = item.DateLastRefreshed;
                                        }

                                    if (item.DateLastSaved == default(DateTime))
                                        {
                                            cmd.GetParameter(index++).Value = null;
                                        } else
                                        {
                                            cmd.GetParameter(index++).Value = item.DateLastSaved;
                                        }

                                    cmd.GetParameter(index++).Value = item.IsInMixedFolder;
                                    cmd.GetParameter(index++).Value = string.Join("|", item.LockedFields.Select(i => i.ToString()).ToArray());
                                    cmd.GetParameter(index++).Value = string.Join("|", item.Studios.ToArray());

                                    if (item.Audio.HasValue)
                                        {
                                            cmd.GetParameter(index++).Value = item.Audio.Value.ToString();
                                        } else
                                        {
                                            cmd.GetParameter(index++).Value = null;
                                        }

                                    cmd.GetParameter(index++).Value = item.ServiceName;

                                    if (item.Tags.Count > 0)
                                        {
                                            cmd.GetParameter(index++).Value = string.Join("|", item.Tags.ToArray());
                                        } else
                                        {
                                            cmd.GetParameter(index++).Value = null;
                                        }

                                    cmd.GetParameter(index++).Value = item.IsFolder;

                                    cmd.GetParameter(index++).Value = item.GetBlockUnratedType().ToString();

                                    var topParent = item.GetTopParent();
                                    if (topParent != null)
                                        {
                                            //Logger.Debug("Item {0} has top parent {1}", item.Id, topParent.Id);
                                            cmd.GetParameter(index++).Value = topParent.Id.ToString("N");
                                        } else
                                        {
                                            //Logger.Debug("Item {0} has null top parent", item.Id);
                                            cmd.GetParameter(index++).Value = null;
                                        }

                                    var isByName = false;
                                    var byName = item as IItemByName;
                                    if (byName != null)
                                        {
                                            var dualAccess = item as IHasDualAccess;
                                            isByName = dualAccess == null || dualAccess.IsAccessedByName;
                                        }
                                    cmd.GetParameter(index++).Value = isByName;

                                    cmd.GetParameter(index++).Value = item.SourceType.ToString();

                                    var trailer = item as Trailer;
                                    if (trailer != null && trailer.TrailerTypes.Count > 0)
                                        {
                                            cmd.GetParameter(index++).Value = string.Join("|", trailer.TrailerTypes.Select(i => i.ToString()).ToArray());
                                        } else
                                        {
                                            cmd.GetParameter(index++).Value = null;
                                        }

                                    cmd.GetParameter(index++).Value = item.CriticRating;
                                    cmd.GetParameter(index++).Value = item.CriticRatingSummary;

                                    var inheritedTags = item.GetInheritedTags();
                                    if (inheritedTags.Count > 0)
                                        {
                                            cmd.GetParameter(index++).Value = string.Join("|", inheritedTags.ToArray());
                                        } else
                                        {
                                            cmd.GetParameter(index++).Value = null;
                                        }

                                    if (string.IsNullOrWhiteSpace(item.Name))
                                        {
                                            cmd.GetParameter(index++).Value = null;
                                        } else
                                        {
                                            cmd.GetParameter(index++).Value = GetCleanValue(item.Name);
                                        }

                                    cmd.GetParameter(index++).Value = item.GetPresentationUniqueKey();
                                    cmd.GetParameter(index++).Value = item.SlugName;
                                    cmd.GetParameter(index++).Value = item.OriginalTitle;

                                    var video = item as Video;
                                    if (video != null)
                                        {
                                            cmd.GetParameter(index++).Value = video.PrimaryVersionId;
                                        } else
                                        {
                                            cmd.GetParameter(index++).Value = null;
                                        }

                                    var folder = item as Folder;
                                    if (folder != null && folder.DateLastMediaAdded.HasValue)
                                        {
                                            cmd.GetParameter(index++).Value = folder.DateLastMediaAdded.Value;
                                        } else
                                        {
                                            cmd.GetParameter(index++).Value = null;
                                        }

                                    cmd.GetParameter(index++).Value = item.Album;

                                    cmd.GetParameter(index++).Value = item.IsVirtualItem;

                                    var hasSeries = item as IHasSeries;
                                    if (hasSeries != null)
                                        {
                                            cmd.GetParameter(index++).Value = hasSeries.FindSeriesName();
                                        } else
                                        {
                                            cmd.GetParameter(index++).Value = null;
                                        }

                                    cmd.GetParameter(index++).Value = item.GetUserDataKeys().FirstOrDefault();

                                    var episode = item as Episode;
                                    if (episode != null)
                                        {
                                            cmd.GetParameter(index++).Value = episode.FindSeasonName();
                                            cmd.GetParameter(index++).Value = episode.FindSeasonId();
                                        } else
                                        {
                                            cmd.GetParameter(index++).Value = null;
                                            cmd.GetParameter(index++).Value = null;
                                        }

                                    if (hasSeries != null)
                                        {
                                            cmd.GetParameter(index++).Value = hasSeries.FindSeriesId();
                                            cmd.GetParameter(index++).Value = hasSeries.FindSeriesSortName();
                                        } else
                                        {
                                            cmd.GetParameter(index++).Value = null;
                                            cmd.GetParameter(index++).Value = null;
                                        }

                                    // Workaround for null values
                                    foreach (IDataParameter param in cmd.Parameters)
                                        {
                                            param.Value = param.Value ?? DBNull.Value;
                                        }

                                    cmd.Transaction = transaction;

                                    cmd.ExecuteNonQuery();

                                    if (item.SupportsAncestors)
                                        {
                                            UpdateAncestors(item.Id, item.GetAncestorIds().Distinct().ToList(), transaction);
                                        }

                                    UpdateUserDataKeys(item.Id, item.GetUserDataKeys().Distinct(StringComparer.OrdinalIgnoreCase).ToList(), transaction);
                                    UpdateImages(item.Id, item.ImageInfos, transaction);
                                    UpdateProviderIds(item.Id, item.ProviderIds, transaction);
                                    UpdateItemValues(item.Id, GetItemValuesToSave(item), transaction);
                                }
                                
                        }


                    transaction.Commit();
                } catch (OperationCanceledException)
                {
                    if (transaction != null)
                        {
                            transaction.Rollback();
                        }

                    throw;
                } catch (Exception e)
                {
                    Logger.ErrorException("Failed to save items:", e);

                    if (transaction != null)
                        {
                            transaction.Rollback();
                        }

                    throw;
                } finally
                {
                    if (transaction != null)
                        {
                            transaction.Dispose();
                        }

                    WriteLock.Release();
                }
        }

        /// <summary>
        /// Internal retrieve from items or users table
        /// </summary>
        /// <param name="id">The id.</param>
        /// <returns>BaseItem.</returns>
        /// <exception cref="System.ArgumentNullException">id</exception>
        /// <exception cref="System.ArgumentException"></exception>
        public BaseItem RetrieveItem(Guid id)
        {
            if (id == Guid.Empty)
                {
                    throw new ArgumentNullException("id");
                }

            CheckDisposed();

            using (var cmd = _connection.CreateCommand())
                {
                    cmd.CommandText = "SELECT " + string.Join(",", _retriveItemColumns) + " FROM TypedBaseItems WHERE guid = @guid";
                    cmd.Parameters.Add(cmd, "@guid", DbType.Guid).Value = id;

                    using (var reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult | CommandBehavior.SingleRow))
                        {
                            if (reader.Read())
                                {
                                    return GetItem(reader);
                                }
                        }
                    return null;
                }
        }

        private BaseItem GetItem(IDataReader reader)
        {
            var typeString = reader.GetString(0);

            var type = _typeMapper.GetType(typeString);

            if (type == null)
                {
                    //Logger.Debug("Unknown type {0}", typeString);

                    return null;
                }

            BaseItem item = null;

            using (var stream = reader.GetMemoryStream(1))
                {
                    try
                        {
                            item = _jsonSerializer.DeserializeFromStream(stream, type) as BaseItem;
                        } catch (SerializationException ex)
                        {
                            Logger.ErrorException("Error deserializing item", ex);
                        }

                    if (item == null)
                        {
                            try
                                {
                                    item = Activator.CreateInstance(type) as BaseItem;
                                } catch
                                {
                                }
                        }

                    if (item == null)
                        {
                            return null;
                        }
                }

            if (!reader.IsDBNull(2))
                {
                    var hasStartDate = item as IHasStartDate;
                    if (hasStartDate != null)
                        {
                            hasStartDate.StartDate = reader.GetDateTime(2).ToUniversalTime();
                        }
                }

            if (!reader.IsDBNull(3))
                {
                    item.EndDate = reader.GetDateTime(3).ToUniversalTime();
                }

            if (!reader.IsDBNull(4))
                {
                    item.IsOffline = reader.GetBoolean(4);
                }

            if (!reader.IsDBNull(5))
                {
                    item.ChannelId = reader.GetString(5);
                }

            var hasProgramAttributes = item as IHasProgramAttributes;
            if (hasProgramAttributes != null)
                {
                    if (!reader.IsDBNull(6))
                        {
                            hasProgramAttributes.IsMovie = reader.GetBoolean(6);
                        }

                    if (!reader.IsDBNull(7))
                        {
                            hasProgramAttributes.IsSports = reader.GetBoolean(7);
                        }

                    if (!reader.IsDBNull(8))
                        {
                            hasProgramAttributes.IsKids = reader.GetBoolean(8);
                        }

                    if (!reader.IsDBNull(9))
                        {
                            hasProgramAttributes.IsSeries = reader.GetBoolean(9);
                        }

                    if (!reader.IsDBNull(10))
                        {
                            hasProgramAttributes.IsLive = reader.GetBoolean(10);
                        }

                    if (!reader.IsDBNull(11))
                        {
                            hasProgramAttributes.IsNews = reader.GetBoolean(11);
                        }

                    if (!reader.IsDBNull(12))
                        {
                            hasProgramAttributes.IsPremiere = reader.GetBoolean(12);
                        }

                    if (!reader.IsDBNull(13))
                        {
                            hasProgramAttributes.EpisodeTitle = reader.GetString(13);
                        }

                    if (!reader.IsDBNull(14))
                        {
                            hasProgramAttributes.IsRepeat = reader.GetBoolean(14);
                        }
                }

            if (!reader.IsDBNull(15))
                {
                    item.CommunityRating = reader.GetFloat(15);
                }

            if (!reader.IsDBNull(16))
                {
                    item.CustomRating = reader.GetString(16);
                }

            if (!reader.IsDBNull(17))
                {
                    item.IndexNumber = reader.GetInt32(17);
                }

            if (!reader.IsDBNull(18))
                {
                    item.IsLocked = reader.GetBoolean(18);
                }

            if (!reader.IsDBNull(19))
                {
                    item.PreferredMetadataLanguage = reader.GetString(19);
                }

            if (!reader.IsDBNull(20))
                {
                    item.PreferredMetadataCountryCode = reader.GetString(20);
                }

            if (!reader.IsDBNull(21))
                {
                    item.IsHD = reader.GetBoolean(21);
                }

            if (!reader.IsDBNull(22))
                {
                    item.ExternalEtag = reader.GetString(22);
                }

            if (!reader.IsDBNull(23))
                {
                    item.DateLastRefreshed = reader.GetDateTime(23).ToUniversalTime();
                }

            if (!reader.IsDBNull(24))
                {
                    item.Name = reader.GetString(24);
                }

            if (!reader.IsDBNull(25))
                {
                    item.Path = reader.GetString(25);
                }

            if (!reader.IsDBNull(26))
                {
                    item.PremiereDate = reader.GetDateTime(26).ToUniversalTime();
                }

            if (!reader.IsDBNull(27))
                {
                    item.Overview = reader.GetString(27);
                }

            if (!reader.IsDBNull(28))
                {
                    item.ParentIndexNumber = reader.GetInt32(28);
                }

            if (!reader.IsDBNull(29))
                {
                    item.ProductionYear = reader.GetInt32(29);
                }

            if (!reader.IsDBNull(30))
                {
                    item.OfficialRating = reader.GetString(30);
                }

            if (!reader.IsDBNull(31))
                {
                    item.OfficialRatingDescription = reader.GetString(31);
                }

            if (!reader.IsDBNull(32))
                {
                    item.HomePageUrl = reader.GetString(32);
                }

            if (!reader.IsDBNull(33))
                {
                    item.DisplayMediaType = reader.GetString(33);
                }

            if (!reader.IsDBNull(34))
                {
                    item.ForcedSortName = reader.GetString(34);
                }

            if (!reader.IsDBNull(35))
                {
                    item.RunTimeTicks = reader.GetInt64(35);
                }

            if (!reader.IsDBNull(36))
                {
                    item.VoteCount = reader.GetInt32(36);
                }

            if (!reader.IsDBNull(37))
                {
                    item.DateCreated = reader.GetDateTime(37).ToUniversalTime();
                }

            if (!reader.IsDBNull(38))
                {
                    item.DateModified = reader.GetDateTime(38).ToUniversalTime();
                }

            item.Id = reader.GetGuid(39);

            if (!reader.IsDBNull(40))
                {
                    item.Genres = reader.GetString(40).Split('|').Where(i => !string.IsNullOrWhiteSpace(i)).ToList();
                }

            if (!reader.IsDBNull(41))
                {
                    item.ParentId = reader.GetGuid(41);
                }

            if (!reader.IsDBNull(42))
                {
                    item.Audio = (ProgramAudio)Enum.Parse(typeof(ProgramAudio), reader.GetString(42), true);
                }

            if (!reader.IsDBNull(43))
                {
                    item.ServiceName = reader.GetString(43);
                }

            if (!reader.IsDBNull(44))
                {
                    item.IsInMixedFolder = reader.GetBoolean(44);
                }

            if (!reader.IsDBNull(45))
                {
                    item.DateLastSaved = reader.GetDateTime(45).ToUniversalTime();
                }

            if (!reader.IsDBNull(46))
                {
                    item.LockedFields = reader.GetString(46).Split('|').Where(i => !string.IsNullOrWhiteSpace(i)).Select(i => (MetadataFields)Enum.Parse(typeof(MetadataFields), i, true)).ToList();
                }

            if (!reader.IsDBNull(47))
                {
                    item.Studios = reader.GetString(47).Split('|').Where(i => !string.IsNullOrWhiteSpace(i)).ToList();
                }

            if (!reader.IsDBNull(48))
                {
                    item.Tags = reader.GetString(48).Split('|').Where(i => !string.IsNullOrWhiteSpace(i)).ToList();
                }

            if (!reader.IsDBNull(49))
                {
                    item.SourceType = (SourceType)Enum.Parse(typeof(SourceType), reader.GetString(49), true);
                }

            var trailer = item as Trailer;
            if (trailer != null)
                {
                    if (!reader.IsDBNull(50))
                        {
                            trailer.TrailerTypes = reader.GetString(50).Split('|').Where(i => !string.IsNullOrWhiteSpace(i)).Select(i => (TrailerType)Enum.Parse(typeof(TrailerType), i, true)).ToList();
                        }
                }

            var index = 51;

            if (!reader.IsDBNull(index))
                {
                    item.OriginalTitle = reader.GetString(index);
                }
            index++;

            var video = item as Video;
            if (video != null)
                {
                    if (!reader.IsDBNull(index))
                        {
                            video.PrimaryVersionId = reader.GetString(index);
                        }
                }
            index++;

            var folder = item as Folder;
            if (folder != null && !reader.IsDBNull(index))
                {
                    folder.DateLastMediaAdded = reader.GetDateTime(index).ToUniversalTime();
                }
            index++;

            if (!reader.IsDBNull(index))
                {
                    item.Album = reader.GetString(index);
                }
            index++;

            if (!reader.IsDBNull(index))
                {
                    item.CriticRating = reader.GetFloat(index);
                }
            index++;

            if (!reader.IsDBNull(index))
                {
                    item.CriticRatingSummary = reader.GetString(index);
                }
            index++;

            if (!reader.IsDBNull(index))
                {
                    item.IsVirtualItem = reader.GetBoolean(index);
                }
            index++;

            var hasSeries = item as IHasSeries;
            if (hasSeries != null)
                {
                    if (!reader.IsDBNull(index))
                        {
                            hasSeries.SeriesName = reader.GetString(index);
                        }
                }
            index++;

            var episode = item as Episode;
            if (episode != null)
                {
                    if (!reader.IsDBNull(index))
                        {
                            episode.SeasonName = reader.GetString(index);
                        }
                    index++;
                    if (!reader.IsDBNull(index))
                        {
                            episode.SeasonId = reader.GetGuid(index);
                        }
                } else
                {
                    index++;
                }
            index++;

            if (hasSeries != null)
                {
                    if (!reader.IsDBNull(index))
                        {
                            hasSeries.SeriesId = reader.GetGuid(index);
                        }
                }
            index++;

            if (hasSeries != null)
                {
                    if (!reader.IsDBNull(index))
                        {
                            hasSeries.SeriesSortName = reader.GetString(index);
                        }
                }
            index++;

            if (!reader.IsDBNull(index))
                {
                    item.PresentationUniqueKey = reader.GetString(index);
                }
            index++;

            if (!reader.IsDBNull(index))
                {
                    item.InheritedParentalRatingValue = reader.GetInt32(index);
                }
            index++;

            return item;
        }

        /// <summary>
        /// Gets the critic reviews.
        /// </summary>
        /// <param name="itemId">The item id.</param>
        /// <returns>Task{IEnumerable{ItemReview}}.</returns>
        public IEnumerable<ItemReview> GetCriticReviews(Guid itemId)
        {
            try
                {
                    var path = Path.Combine(_criticReviewsPath, itemId + ".json");

                    return _jsonSerializer.DeserializeFromFile<List<ItemReview>>(path);
                } catch (DirectoryNotFoundException)
                {
                    return new List<ItemReview>();
                } catch (FileNotFoundException)
                {
                    return new List<ItemReview>();
                }
        }

        private readonly Task _cachedTask = Task.FromResult(true);

        /// <summary>
        /// Saves the critic reviews.
        /// </summary>
        /// <param name="itemId">The item id.</param>
        /// <param name="criticReviews">The critic reviews.</param>
        /// <returns>Task.</returns>
        public Task SaveCriticReviews(Guid itemId, IEnumerable<ItemReview> criticReviews)
        {
            Directory.CreateDirectory(_criticReviewsPath);

            var path = Path.Combine(_criticReviewsPath, itemId + ".json");

            _jsonSerializer.SerializeToFile(criticReviews.ToList(), path);

            return _cachedTask;
        }

        /// <summary>
        /// Gets chapters for an item
        /// </summary>
        /// <param name="id">The id.</param>
        /// <returns>IEnumerable{ChapterInfo}.</returns>
        /// <exception cref="System.ArgumentNullException">id</exception>
        public IEnumerable<ChapterInfo> GetChapters(Guid id)
        {
            CheckDisposed();
            if (id == Guid.Empty)
                {
                    throw new ArgumentNullException("id");
                }
            var list = new List<ChapterInfo>();

            using (var cmd = _connection.CreateCommand())
                {
                    cmd.CommandText = "SELECT StartPositionTicks, Name, ImagePath, ImageDateModified FROM " + ChaptersTableName + " WHERE ItemId = @ItemId ORDER BY ChapterIndex ASC";

                    cmd.Parameters.Add(cmd, "@ItemId", DbType.Guid).Value = id;

                    using (var reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult))
                        {
                            while (reader.Read())
                                {
                                    list.Add(GetChapter(reader));
                                }
                        }
                }

            return list;
        }

        /// <summary>
        /// Gets a single chapter for an item
        /// </summary>
        /// <param name="id">The id.</param>
        /// <param name="index">The index.</param>
        /// <returns>ChapterInfo.</returns>
        /// <exception cref="System.ArgumentNullException">id</exception>
        public ChapterInfo GetChapter(Guid id, int index)
        {
            CheckDisposed();
            if (id == Guid.Empty)
                {
                    throw new ArgumentNullException("id");
                }

            using (var cmd = _connection.CreateCommand())
                {
                    cmd.CommandText = "SELECT StartPositionTicks, Name, ImagePath, ImageDateModified FROM " + ChaptersTableName + " WHERE ItemId = @ItemId AND ChapterIndex = @ChapterIndex";

                    cmd.Parameters.Add(cmd, "@ItemId", DbType.Guid).Value = id;
                    cmd.Parameters.Add(cmd, "@ChapterIndex", DbType.Int32).Value = index;

                    using (var reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult | CommandBehavior.SingleRow))
                        {
                            if (reader.Read())
                                {
                                    return GetChapter(reader);
                                }
                        }
                    return null;
                }
        }

        /// <summary>
        /// Gets the chapter.
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <returns>ChapterInfo.</returns>
        private ChapterInfo GetChapter(IDataReader reader)
        {
            var chapter = new ChapterInfo {
                StartPositionTicks = reader.GetInt64(0)
            };

            if (!reader.IsDBNull(1))
                {
                    chapter.Name = reader.GetString(1);
                }

            if (!reader.IsDBNull(2))
                {
                    chapter.ImagePath = reader.GetString(2);
                }

            if (!reader.IsDBNull(3))
                {
                    chapter.ImageDateModified = reader.GetDateTime(3).ToUniversalTime();
                }

            return chapter;
        }

        /// <summary>
        /// Saves the chapters.
        /// </summary>
        /// <param name="id">The id.</param>
        /// <param name="chapters">The chapters.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>Task.</returns>
        /// <exception cref="System.ArgumentNullException">
        /// id
        /// or
        /// chapters
        /// or
        /// cancellationToken
        /// </exception>
        public async Task SaveChapters(Guid id, List<ChapterInfo> chapters, CancellationToken cancellationToken)
        {
            CheckDisposed();

            if (id == Guid.Empty)
                {
                    throw new ArgumentNullException("id");
                }

            if (chapters == null)
                {
                    throw new ArgumentNullException("chapters");
                }

            cancellationToken.ThrowIfCancellationRequested();

            await WriteLock.WaitAsync(cancellationToken).ConfigureAwait(false);

            IDbTransaction transaction = null;

            try
                {
                    transaction = _connection.BeginTransaction();

                    // First delete chapters
                    _deleteChaptersCommand.GetParameter(0).Value = id;

                    _deleteChaptersCommand.Transaction = transaction;

                    _deleteChaptersCommand.ExecuteNonQuery();

                    var index = 0;

                    foreach (var chapter in chapters)
                        {
                            cancellationToken.ThrowIfCancellationRequested();

                            _saveChapterCommand.GetParameter(0).Value = id;
                            _saveChapterCommand.GetParameter(1).Value = index;
                            _saveChapterCommand.GetParameter(2).Value = chapter.StartPositionTicks;
                            _saveChapterCommand.GetParameter(3).Value = chapter.Name;
                            _saveChapterCommand.GetParameter(4).Value = chapter.ImagePath;
                            _saveChapterCommand.GetParameter(5).Value = chapter.ImageDateModified;

                            _saveChapterCommand.Transaction = transaction;

                            _saveChapterCommand.ExecuteNonQuery();

                            index++;
                        }

                    transaction.Commit();
                } catch (OperationCanceledException)
                {
                    if (transaction != null)
                        {
                            transaction.Rollback();
                        }

                    throw;
                } catch (Exception e)
                {
                    Logger.ErrorException("Failed to save chapters:", e);

                    if (transaction != null)
                        {
                            transaction.Rollback();
                        }

                    throw;
                } finally
                {
                    if (transaction != null)
                        {
                            transaction.Dispose();
                        }

                    WriteLock.Release();
                }
        }

        protected override void CloseConnection()
        {
            if (_connection != null)
                {
                    if (_connection.IsOpen())
                        {
                            _connection.Close();
                        }

                    _connection.Dispose();
                    _connection = null;
                }
        }

        private bool EnableJoinUserData(InternalItemsQuery query)
        {
            if (query.User == null)
                {
                    return false;
                }

            if (query.SimilarTo != null && query.User != null)
                {
                    return true;
                }

            if (query.SortBy != null && query.SortBy.Length > 0)
                {
                    if (query.SortBy.Contains(ItemSortBy.IsFavoriteOrLiked, StringComparer.OrdinalIgnoreCase))
                        {
                            return true;
                        }
                    if (query.SortBy.Contains(ItemSortBy.IsPlayed, StringComparer.OrdinalIgnoreCase))
                        {
                            return true;
                        }
                    if (query.SortBy.Contains(ItemSortBy.IsUnplayed, StringComparer.OrdinalIgnoreCase))
                        {
                            return true;
                        }
                    if (query.SortBy.Contains(ItemSortBy.PlayCount, StringComparer.OrdinalIgnoreCase))
                        {
                            return true;
                        }
                    if (query.SortBy.Contains(ItemSortBy.DatePlayed, StringComparer.OrdinalIgnoreCase))
                        {
                            return true;
                        }
                }

            if (query.IsFavoriteOrLiked.HasValue)
                {
                    return true;
                }

            if (query.IsFavorite.HasValue)
                {
                    return true;
                }

            if (query.IsResumable.HasValue)
                {
                    return true;
                }

            if (query.IsPlayed.HasValue)
                {
                    return true;
                }

            if (query.IsLiked.HasValue)
                {
                    return true;
                }

            return false;
        }

        private string[] GetFinalColumnsToSelect(InternalItemsQuery query, string[] startColumns, IDbCommand cmd)
        {
            var list = startColumns.ToList();

            if (EnableJoinUserData(query))
                {
                    list.Add("UserDataDb.UserData.UserId");
                    list.Add("UserDataDb.UserData.lastPlayedDate");
                    list.Add("UserDataDb.UserData.playbackPositionTicks");
                    list.Add("UserDataDb.UserData.playcount");
                    list.Add("UserDataDb.UserData.isFavorite");
                    list.Add("UserDataDb.UserData.played");
                    list.Add("UserDataDb.UserData.rating");
                }

            if (query.SimilarTo != null)
                {
                    var item = query.SimilarTo;

                    var builder = new StringBuilder();
                    builder.Append("(");

                    builder.Append("((OfficialRating = @ItemOfficialRating) * 10)");
                    //builder.Append(" + ((ProductionYear = @ItemProductionYear) * 10)");

                    builder.Append(" + (SELECT CASE WHEN ABS(COALESCE(ProductionYear, 0) - @ItemProductionYear) < 10 THEN 2 ELSE 0 END)");
                    builder.Append(" + (SELECT CASE WHEN ABS(COALESCE(ProductionYear, 0) - @ItemProductionYear) < 5 THEN 2 ELSE 0 END)");

                    //// genres
                    builder.Append(" + ((SELECT COUNT(CleanValue) FROM ItemValues WHERE ItemId = Guid AND Type = 2 AND CleanValue IN (SELECT CleanValue FROM ItemValues WHERE ItemId = @SimilarItemId AND Type = 2)) * 10)");

                    //// tags
                    builder.Append(" + ((SELECT COUNT(CleanValue) FROM ItemValues WHERE ItemId = Guid AND Type = 4 AND CleanValue IN (SELECT CleanValue FROM ItemValues WHERE ItemId = @SimilarItemId AND Type = 4)) * 10)");

                    builder.Append(" + ((SELECT COUNT(CleanValue) FROM ItemValues WHERE ItemId = Guid AND Type = 5 AND CleanValue IN (SELECT CleanValue FROM ItemValues WHERE ItemId = @SimilarItemId AND Type = 5)) * 10)");

                    builder.Append(" + ((SELECT COUNT(CleanValue) FROM ItemValues WHERE ItemId = Guid AND Type = 3 AND CleanValue IN (SELECT CleanValue FROM ItemValues WHERE ItemId = @SimilarItemId AND Type = 3)) * 3)");

                    //builder.Append(" + ((SELECT COUNT(Name) FROM People WHERE ItemId = Guid AND Name IN (SELECT Name FROM People WHERE ItemId = @SimilarItemId)) * 3)");

                    ////builder.Append(" (SELECT STRING_AGG((SELECT Name FROM People WHERE ItemId = Guid AND Name IN (SELECT Name FROM People WHERE ItemId = @SimilarItemId)), '|'))");

                    builder.Append(") AS SimilarityScore");

                    list.Add(builder.ToString());
                    cmd.Parameters.Add(cmd, "@ItemOfficialRating", DbType.String).Value = item.OfficialRating;
                    cmd.Parameters.Add(cmd, "@ItemProductionYear", DbType.Int32).Value = item.ProductionYear ?? 0;
                    cmd.Parameters.Add(cmd, "@SimilarItemId", DbType.Guid).Value = item.Id;

                    var excludeIds = query.ExcludeItemIds.ToList();
                    excludeIds.Add(item.Id.ToString("N"));
                    query.ExcludeItemIds = excludeIds.ToArray();

                    query.ExcludeProviderIds = item.ProviderIds;
                }

            return list.ToArray();
        }

        private string GetJoinUserDataText(InternalItemsQuery query)
        {
            if (!EnableJoinUserData(query))
                {
                    return string.Empty;
                }

            if (_config.Configuration.SchemaVersion >= 96)
                {
                    return " LEFT JOIN UserData ON UserDataKey = UserData.Key AND (UserId = @UserId)";
                }

            return " LEFT JOIN UserData ON (SELECT UserDataKey FROM UserDataKeys WHERE ItemId = Guid ORDER BY Priority LIMIT 1) = UserData.Key AND (UserId = @UserId)";
        }

        private string GetGroupBy(InternalItemsQuery query)
        {
            var groups = new List<string>();

            if (EnableGroupByPresentationUniqueKey(query))
                {
                    groups.Add("PresentationUniqueKey");
                }

            if (groups.Count > 0)
                {
                    return " GROUP BY " + string.Join(",", groups.ToArray());
                }

            return string.Empty;
        }

        private string GetFromText(string alias = "A")
        {
            return " FROM TypedBaseItems " + alias;
        }

        public List<BaseItem> GetItemList(InternalItemsQuery query)
        {
            if (query == null)
                {
                    throw new ArgumentNullException("query");
                }

            CheckDisposed();

            var now = DateTime.UtcNow;

            var list = new List<BaseItem>();

            // Hack for right now since we currently don't support filtering out these duplicates within a query
            if (query.Limit.HasValue && query.EnableGroupByMetadataKey)
                {
                    query.Limit = query.Limit.Value + 4;
                }

            using (var cmd = _connection.CreateCommand())
                {
                    cmd.CommandText = "SELECT " + string.Join(",", GetFinalColumnsToSelect(query, _retriveItemColumns, cmd)) + GetFromText();
                    cmd.CommandText += GetJoinUserDataText(query);

                    if (EnableJoinUserData(query))
                        {
                            cmd.Parameters.Add(cmd, "@UserId", DbType.Guid).Value = query.User.Id;
                        }

                    var whereClauses = GetWhereClauses(query, cmd);

                    var whereText = whereClauses.Count == 0 ?
                    string.Empty :
                    " WHERE " + string.Join(" AND ", whereClauses.ToArray());

                    cmd.CommandText += whereText;

                    cmd.CommandText += GetGroupBy(query);

                    cmd.CommandText += GetOrderByText(query);

                    if (query.Limit.HasValue || query.StartIndex.HasValue)
                        {
                            var offset = query.StartIndex ?? 0;

                            if (query.Limit.HasValue || offset > 0)
                                {
                                    cmd.CommandText += " LIMIT " + (query.Limit ?? int.MaxValue).ToString(CultureInfo.InvariantCulture);
                                }

                            if (offset > 0)
                                {
                                    cmd.CommandText += " OFFSET " + offset.ToString(CultureInfo.InvariantCulture);
                                }
                        }

                    using (var reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult))
                        {
                            LogQueryTime("GetItemList", cmd, now);

                            while (reader.Read())
                                {
                                    var item = GetItem(reader);
                                    if (item != null)
                                        {
                                            list.Add(item);
                                        }
                                }
                        }
                }

            // Hack for right now since we currently don't support filtering out these duplicates within a query
            if (query.EnableGroupByMetadataKey)
                {
                    var limit = query.Limit ?? int.MaxValue;
                    limit -= 4;
                    var newList = new List<BaseItem>();

                    foreach (var item in list)
                        {
                            AddItem(newList, item);

                            if (newList.Count >= limit)
                                {
                                    break;
                                }
                        }

                    list = newList;
                }

            return list;
        }

        private void AddItem(List<BaseItem> items, BaseItem newItem)
        {
            var providerIds = newItem.ProviderIds.ToList();

            for (var i = 0; i < items.Count; i++)
                {
                    var item = items[i];

                    foreach (var providerId in providerIds)
                        {
                            if (providerId.Key == MetadataProviders.TmdbCollection.ToString())
                                {
                                    continue;
                                }
                            if (item.GetProviderId(providerId.Key) == providerId.Value)
                                {
                                    if (newItem.SourceType == SourceType.Library)
                                        {
                                            items[i] = newItem;
                                        }
                                    return;
                                }
                        }
                }

            items.Add(newItem);
        }

        private void LogQueryTime(string methodName, IDbCommand cmd, DateTime startDate)
        {
            var elapsed = (DateTime.UtcNow - startDate).TotalMilliseconds;

            var slowThreshold = 1000;

#if DEBUG
            slowThreshold = 50;
#endif

            if (elapsed >= slowThreshold)
                {
                    Logger.Debug("{2} query time (slow): {0}ms. Query: {1}",
                        Convert.ToInt32(elapsed),
                        cmd.CommandText,
                        methodName);
                } else
                {
                    //Logger.Debug("{2} query time: {0}ms. Query: {1}",
                    //    Convert.ToInt32(elapsed),
                    //    cmd.CommandText,
                    //    methodName);
                }
        }

        public QueryResult<BaseItem> GetItems(InternalItemsQuery query)
        {
            if (query == null)
                {
                    throw new ArgumentNullException("query");
                }

            CheckDisposed();

            if (!query.EnableTotalRecordCount || (!query.Limit.HasValue && (query.StartIndex ?? 0) == 0))
                {
                    var list = GetItemList(query);
                    return new QueryResult<BaseItem> {
                        Items = list.ToArray(),
                        TotalRecordCount = list.Count
                    };
                }

            var now = DateTime.UtcNow;

            using (var cmd = _connection.CreateCommand())
                {
                    cmd.CommandText = "SELECT " + string.Join(",", GetFinalColumnsToSelect(query, _retriveItemColumns, cmd)) + GetFromText();
                    cmd.CommandText += GetJoinUserDataText(query);

                    if (EnableJoinUserData(query))
                        {
                            cmd.Parameters.Add(cmd, "@UserId", DbType.Guid).Value = query.User.Id;
                        }

                    var whereClauses = GetWhereClauses(query, cmd);

                    var whereTextWithoutPaging = whereClauses.Count == 0 ?
                    string.Empty :
                    " WHERE " + string.Join(" AND ", whereClauses.ToArray());

                    var whereText = whereClauses.Count == 0 ?
                    string.Empty :
                    " WHERE " + string.Join(" AND ", whereClauses.ToArray());

                    cmd.CommandText += whereText;

                    cmd.CommandText += GetGroupBy(query);

                    cmd.CommandText += GetOrderByText(query);

                    if (query.Limit.HasValue || query.StartIndex.HasValue)
                        {
                            var offset = query.StartIndex ?? 0;

                            if (query.Limit.HasValue || offset > 0)
                                {
                                    cmd.CommandText += " LIMIT " + (query.Limit ?? int.MaxValue).ToString(CultureInfo.InvariantCulture);
                                }

                            if (offset > 0)
                                {
                                    cmd.CommandText += " OFFSET " + offset.ToString(CultureInfo.InvariantCulture);
                                }
                        }

                    cmd.CommandText += ";";

                    var isReturningZeroItems = query.Limit.HasValue && query.Limit <= 0;

                    if (isReturningZeroItems)
                        {
                            cmd.CommandText = "";
                        }

                    if (EnableGroupByPresentationUniqueKey(query))
                        {
                            cmd.CommandText += " SELECT COUNT(DISTINCT PresentationUniqueKey)" + GetFromText();
                        } else
                        {
                            cmd.CommandText += " SELECT COUNT(guid)" + GetFromText();
                        }

                    cmd.CommandText += GetJoinUserDataText(query);
                    cmd.CommandText += whereTextWithoutPaging;

                    var list = new List<BaseItem>();
                    var count = 0;

                    using (var reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess))
                        {
                            LogQueryTime("GetItems", cmd, now);

                            if (isReturningZeroItems)
                                {
                                    if (reader.Read())
                                        {
                                            count = reader.GetInt32(0);
                                        }
                                } else
                                {
                                    while (reader.Read())
                                        {
                                            var item = GetItem(reader);
                                            if (item != null)
                                                {
                                                    list.Add(item);
                                                }
                                        }

                                    if (reader.NextResult() && reader.Read())
                                        {
                                            count = reader.GetInt32(0);
                                        }
                                }
                        }

                    return new QueryResult<BaseItem>() {
                        Items = list.ToArray(),
                        TotalRecordCount = count
                    };
                }
        }

        private string GetOrderByText(InternalItemsQuery query)
        {
            if (query.SimilarTo != null)
                {
                    if (query.SortBy == null || query.SortBy.Length == 0)
                        {
                            if (query.User != null)
                                {
                                    query.SortBy = new[] {
                                        "SimilarityScore",
                                        ItemSortBy.Random
                                    };
                                } else
                                {
                                    query.SortBy = new[] {
                                        "SimilarityScore",
                                        ItemSortBy.Random
                                    };
                                }
                            query.SortOrder = SortOrder.Descending;
                        }
                }

            if (query.SortBy == null || query.SortBy.Length == 0)
                {
                    return string.Empty;
                }

            var isAscending = query.SortOrder != SortOrder.Descending;

            return " ORDER BY " + string.Join(",", query.SortBy.Select(i => {
                var columnMap = MapOrderByField(i, query);
                var columnAscending = isAscending;
                if (columnMap.Item2)
                    {
                        columnAscending = !columnAscending;
                    }

                var sortOrder = columnAscending ? "ASC" : "DESC";

                return columnMap.Item1 + " " + sortOrder;
            }).ToArray());
        }

        private Tuple<string, bool> MapOrderByField(string name, InternalItemsQuery query)
        {
            if (string.Equals(name, ItemSortBy.AirTime, StringComparison.OrdinalIgnoreCase))
                {
                    // TODO
                    return new Tuple<string, bool>("SortName", false);
                }
            if (string.Equals(name, ItemSortBy.Runtime, StringComparison.OrdinalIgnoreCase))
                {
                    return new Tuple<string, bool>("RuntimeTicks", false);
                }
            if (string.Equals(name, ItemSortBy.Random, StringComparison.OrdinalIgnoreCase))
                {
                    return new Tuple<string, bool>("RANDOM()", false);
                }
            if (string.Equals(name, ItemSortBy.DatePlayed, StringComparison.OrdinalIgnoreCase))
                {
                    return new Tuple<string, bool>("LastPlayedDate", false);
                }
            if (string.Equals(name, ItemSortBy.PlayCount, StringComparison.OrdinalIgnoreCase))
                {
                    return new Tuple<string, bool>("PlayCount", false);
                }
            if (string.Equals(name, ItemSortBy.IsFavoriteOrLiked, StringComparison.OrdinalIgnoreCase))
                {
                    return new Tuple<string, bool>("IsFavorite", true);
                }
            if (string.Equals(name, ItemSortBy.IsFolder, StringComparison.OrdinalIgnoreCase))
                {
                    return new Tuple<string, bool>("IsFolder", true);
                }
            if (string.Equals(name, ItemSortBy.IsPlayed, StringComparison.OrdinalIgnoreCase))
                {
                    return new Tuple<string, bool>("played", true);
                }
            if (string.Equals(name, ItemSortBy.IsUnplayed, StringComparison.OrdinalIgnoreCase))
                {
                    return new Tuple<string, bool>("played", false);
                }
            if (string.Equals(name, ItemSortBy.DateLastContentAdded, StringComparison.OrdinalIgnoreCase))
                {
                    return new Tuple<string, bool>("DateLastMediaAdded", false);
                }
            if (string.Equals(name, ItemSortBy.Artist, StringComparison.OrdinalIgnoreCase))
                {
                    return new Tuple<string, bool>("(SELECT CleanValue FROM ItemValues WHERE ItemId = Guid AND Type = 0 LIMIT 1)", false);
                }
            if (string.Equals(name, ItemSortBy.AlbumArtist, StringComparison.OrdinalIgnoreCase))
                {
                    return new Tuple<string, bool>("(SELECT CleanValue FROM ItemValues WHERE ItemId = Guid and Type = 1 LIMIT 1)", false);
                }
            if (string.Equals(name, ItemSortBy.OfficialRating, StringComparison.OrdinalIgnoreCase))
                {
                    return new Tuple<string, bool>("InheritedParentalRatingValue", false);
                }
            if (string.Equals(name, ItemSortBy.Studio, StringComparison.OrdinalIgnoreCase))
                {
                    return new Tuple<string, bool>("(SELECT CleanValue FROM ItemValues WHERE ItemId = Guid AND Type = 3 LIMIT 1)", false);
                }
            if (string.Equals(name, ItemSortBy.SeriesDatePlayed, StringComparison.OrdinalIgnoreCase))
                {
                    return new Tuple<string, bool>("(SELECT MAX(LastPlayedDate) FROM TypedBaseItems B" + GetJoinUserDataText(query) + " WHERE B.Guid in (SELECT ItemId FROM AncestorIds WHERE AncestorId IN (SELECT guid FROM TypedBaseItems C WHERE C.Type = 'MediaBrowser.Controller.Entities.TV.Series' AND C.Guid IN (SELECT AncestorId FROM AncestorIds WHERE ItemId = A.Guid))))", false);
                }

            return new Tuple<string, bool>(name, false);
        }

        public List<Guid> GetItemIdsList(InternalItemsQuery query)
        {
            if (query == null)
                {
                    throw new ArgumentNullException("query");
                }

            CheckDisposed();

            var now = DateTime.UtcNow;

            using (var cmd = _connection.CreateCommand())
                {
                    cmd.CommandText = "SELECT " + string.Join(",", GetFinalColumnsToSelect(query, new[] { "guid" }, cmd)) + GetFromText();
                    cmd.CommandText += GetJoinUserDataText(query);

                    if (EnableJoinUserData(query))
                        {
                            cmd.Parameters.Add(cmd, "@UserId", DbType.Guid).Value = query.User.Id;
                        }

                    var whereClauses = GetWhereClauses(query, cmd);

                    var whereText = whereClauses.Count == 0 ?
                    string.Empty :
                    " WHERE " + string.Join(" AND ", whereClauses.ToArray());

                    cmd.CommandText += whereText;

                    cmd.CommandText += GetGroupBy(query);

                    cmd.CommandText += GetOrderByText(query);

                    if (query.Limit.HasValue || query.StartIndex.HasValue)
                        {
                            var offset = query.StartIndex ?? 0;

                            if (query.Limit.HasValue || offset > 0)
                                {
                                    cmd.CommandText += " LIMIT " + (query.Limit ?? int.MaxValue).ToString(CultureInfo.InvariantCulture);
                                }

                            if (offset > 0)
                                {
                                    cmd.CommandText += " OFFSET " + offset.ToString(CultureInfo.InvariantCulture);
                                }
                        }

                    var list = new List<Guid>();

                    using (var reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult))
                        {
                            LogQueryTime("GetItemIdsList", cmd, now);

                            while (reader.Read())
                                {
                                    list.Add(reader.GetGuid(0));
                                }
                        }

                    return list;
                }
        }

        public QueryResult<Tuple<Guid, string>> GetItemIdsWithPath(InternalItemsQuery query)
        {
            if (query == null)
                {
                    throw new ArgumentNullException("query");
                }

            CheckDisposed();

            using (var cmd = _connection.CreateCommand())
                {
                    cmd.CommandText = "SELECT guid, path FROM TypedBaseItems";

                    var whereClauses = GetWhereClauses(query, cmd);

                    var whereTextWithoutPaging = whereClauses.Count == 0 ?
                    string.Empty :
                    " WHERE " + string.Join(" AND ", whereClauses.ToArray());

                    var whereText = whereClauses.Count == 0 ?
                    string.Empty :
                    " WHERE " + string.Join(" AND ", whereClauses.ToArray());

                    cmd.CommandText += whereText;

                    cmd.CommandText += GetGroupBy(query);

                    cmd.CommandText += GetOrderByText(query);

                    if (query.Limit.HasValue || query.StartIndex.HasValue)
                        {
                            var offset = query.StartIndex ?? 0;

                            if (query.Limit.HasValue || offset > 0)
                                {
                                    cmd.CommandText += " LIMIT " + (query.Limit ?? int.MaxValue).ToString(CultureInfo.InvariantCulture);
                                }

                            if (offset > 0)
                                {
                                    cmd.CommandText += " OFFSET " + offset.ToString(CultureInfo.InvariantCulture);
                                }
                        }

                    cmd.CommandText += "; SELECT COUNT(guid) from TypedBaseItems" + whereTextWithoutPaging;

                    var list = new List<Tuple<Guid, string>>();
                    var count = 0;

                    Logger.Debug(cmd.CommandText);

                    using (var reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess))
                        {
                            while (reader.Read())
                                {
                                    var id = reader.GetGuid(0);
                                    string path = null;

                                    if (!reader.IsDBNull(1))
                                        {
                                            path = reader.GetString(1);
                                        }
                                    list.Add(new Tuple<Guid, string>(id, path));
                                }

                            if (reader.NextResult() && reader.Read())
                                {
                                    count = reader.GetInt32(0);
                                }
                        }

                    return new QueryResult<Tuple<Guid, string>>() {
                        Items = list.ToArray(),
                        TotalRecordCount = count
                    };
                }
        }

        public QueryResult<Guid> GetItemIds(InternalItemsQuery query)
        {
            if (query == null)
                {
                    throw new ArgumentNullException("query");
                }

            CheckDisposed();

            if (!query.EnableTotalRecordCount || (!query.Limit.HasValue && (query.StartIndex ?? 0) == 0))
                {
                    var list = GetItemIdsList(query);
                    return new QueryResult<Guid> {
                        Items = list.ToArray(),
                        TotalRecordCount = list.Count
                    };
                }

            var now = DateTime.UtcNow;

            using (var cmd = _connection.CreateCommand())
                {
                    cmd.CommandText = "SELECT " + string.Join(",", GetFinalColumnsToSelect(query, new[] { "guid" }, cmd)) + GetFromText();

                    var whereClauses = GetWhereClauses(query, cmd);
                    cmd.CommandText += GetJoinUserDataText(query);

                    if (EnableJoinUserData(query))
                        {
                            cmd.Parameters.Add(cmd, "@UserId", DbType.Guid).Value = query.User.Id;
                        }

                    var whereText = whereClauses.Count == 0 ?
                    string.Empty :
                    " WHERE " + string.Join(" AND ", whereClauses.ToArray());

                    cmd.CommandText += whereText;

                    cmd.CommandText += GetGroupBy(query);

                    cmd.CommandText += GetOrderByText(query);

                    if (query.Limit.HasValue || query.StartIndex.HasValue)
                        {
                            var offset = query.StartIndex ?? 0;

                            if (query.Limit.HasValue || offset > 0)
                                {
                                    cmd.CommandText += " LIMIT " + (query.Limit ?? int.MaxValue).ToString(CultureInfo.InvariantCulture);
                                }

                            if (offset > 0)
                                {
                                    cmd.CommandText += " OFFSET " + offset.ToString(CultureInfo.InvariantCulture);
                                }
                        }

                    if (EnableGroupByPresentationUniqueKey(query))
                        {
                            cmd.CommandText += "; SELECT COUNT(DISTINCT PresentationUniqueKey)" + GetFromText();
                        } else
                        {
                            cmd.CommandText += "; SELECT COUNT(guid)" + GetFromText();
                        }

                    cmd.CommandText += GetJoinUserDataText(query);
                    cmd.CommandText += whereText;

                    var list = new List<Guid>();
                    var count = 0;

                    using (var reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess))
                        {
                            LogQueryTime("GetItemIds", cmd, now);

                            while (reader.Read())
                                {
                                    list.Add(reader.GetGuid(0));
                                }

                            if (reader.NextResult() && reader.Read())
                                {
                                    count = reader.GetInt32(0);
                                }
                        }

                    return new QueryResult<Guid>() {
                        Items = list.ToArray(),
                        TotalRecordCount = count
                    };
                }
        }

        private List<string> GetWhereClauses(InternalItemsQuery query, IDbCommand cmd, string paramSuffix = "")
        {
            var whereClauses = new List<string>();

            if (EnableJoinUserData(query))
                {
                    //whereClauses.Add("(UserId is null or UserId=@UserId)");
                }
            if (query.IsCurrentSchema.HasValue)
                {
                    if (query.IsCurrentSchema.Value)
                        {
                            whereClauses.Add("(SchemaVersion NOT NULL AND SchemaVersion = @SchemaVersion)");
                        } else
                        {
                            whereClauses.Add("(SchemaVersion IS NULL OR SchemaVersion <> @SchemaVersion)");
                        }
                    cmd.Parameters.Add(cmd, "@SchemaVersion", DbType.Int32).Value = LatestSchemaVersion;
                }
            if (query.IsHD.HasValue)
                {
                    whereClauses.Add("IsHD = @IsHD");
                    cmd.Parameters.Add(cmd, "@IsHD", DbType.Boolean).Value = query.IsHD;
                }
            if (query.IsLocked.HasValue)
                {
                    whereClauses.Add("IsLocked = @IsLocked");
                    cmd.Parameters.Add(cmd, "@IsLocked", DbType.Boolean).Value = query.IsLocked;
                }
            if (query.IsOffline.HasValue)
                {
                    whereClauses.Add("IsOffline = @IsOffline");
                    cmd.Parameters.Add(cmd, "@IsOffline", DbType.Boolean).Value = query.IsOffline;
                }
            if (query.IsMovie.HasValue)
                {
                    var alternateTypes = new List<string>();
                    if (query.IncludeItemTypes.Length == 0 || query.IncludeItemTypes.Contains(typeof(Movie).Name))
                        {
                            alternateTypes.Add(typeof(Movie).FullName);
                        }
                    if (query.IncludeItemTypes.Length == 0 || query.IncludeItemTypes.Contains(typeof(Trailer).Name))
                        {
                            alternateTypes.Add(typeof(Trailer).FullName);
                        }

                    if (alternateTypes.Count == 0)
                        {
                            whereClauses.Add("IsMovie = @IsMovie");
                        } else
                        {
                            whereClauses.Add("(IsMovie IS NULL OR IsMovie = @IsMovie)");
                        }
                    cmd.Parameters.Add(cmd, "@IsMovie", DbType.Boolean).Value = query.IsMovie;
                }
            if (query.IsKids.HasValue)
                {
                    whereClauses.Add("IsKids = @IsKids");
                    cmd.Parameters.Add(cmd, "@IsKids", DbType.Boolean).Value = query.IsKids;
                }
            if (query.IsSports.HasValue)
                {
                    whereClauses.Add("IsSports = @IsSports");
                    cmd.Parameters.Add(cmd, "@IsSports", DbType.Boolean).Value = query.IsSports;
                }
            if (query.IsFolder.HasValue)
                {
                    whereClauses.Add("IsFolder = @IsFolder");
                    cmd.Parameters.Add(cmd, "@IsFolder", DbType.Boolean).Value = query.IsFolder;
                }

            var includeTypes = query.IncludeItemTypes.SelectMany(MapIncludeItemTypes).ToArray();
            if (includeTypes.Length == 1)
                {
                    whereClauses.Add("type = @type" + paramSuffix);
                    cmd.Parameters.Add(cmd, "@type" + paramSuffix, DbType.String).Value = includeTypes[0];
                } else if (includeTypes.Length > 1)
                {
                    var inClause = string.Join(",", includeTypes.Select(i => "'" + i + "'").ToArray());
                    whereClauses.Add(string.Format("type IN ({0})", inClause));
                }

            var excludeTypes = query.ExcludeItemTypes.SelectMany(MapIncludeItemTypes).ToArray();
            if (excludeTypes.Length == 1)
                {
                    whereClauses.Add("type <> @type");
                    cmd.Parameters.Add(cmd, "@type", DbType.String).Value = excludeTypes[0];
                } else if (excludeTypes.Length > 1)
                {
                    var inClause = string.Join(",", excludeTypes.Select(i => "'" + i + "'").ToArray());
                    whereClauses.Add(string.Format("type NOT IN ({0})", inClause));
                }

            if (query.ChannelIds.Length == 1)
                {
                    whereClauses.Add("ChannelId = @ChannelId");
                    cmd.Parameters.Add(cmd, "@ChannelId", DbType.String).Value = query.ChannelIds[0];
                }
            if (query.ChannelIds.Length > 1)
                {
                    var inClause = string.Join(",", query.ChannelIds.Select(i => "'" + i + "'").ToArray());
                    whereClauses.Add(string.Format("ChannelId IN ({0})", inClause));
                }

            if (query.ParentId.HasValue)
                {
                    whereClauses.Add("ParentId = @ParentId");
                    cmd.Parameters.Add(cmd, "@ParentId", DbType.Guid).Value = query.ParentId.Value;
                }

            if (!string.IsNullOrWhiteSpace(query.Path))
                {
                    whereClauses.Add("Path = @Path");
                    cmd.Parameters.Add(cmd, "@Path", DbType.String).Value = query.Path;
                }

            if (!string.IsNullOrWhiteSpace(query.PresentationUniqueKey))
                {
                    whereClauses.Add("PresentationUniqueKey = @PresentationUniqueKey");
                    cmd.Parameters.Add(cmd, "@PresentationUniqueKey", DbType.String).Value = query.PresentationUniqueKey;
                }

            if (query.MinCommunityRating.HasValue)
                {
                    whereClauses.Add("CommunityRating >= @MinCommunityRating");
                    cmd.Parameters.Add(cmd, "@MinCommunityRating", DbType.Double).Value = query.MinCommunityRating.Value;
                }

            if (query.MinIndexNumber.HasValue)
                {
                    whereClauses.Add("IndexNumber >= @MinIndexNumber");
                    cmd.Parameters.Add(cmd, "@MinIndexNumber", DbType.Int32).Value = query.MinIndexNumber.Value;
                }

            //if (query.MinPlayers.HasValue)
            //{
            //    whereClauses.Add("Players>=@MinPlayers");
            //    cmd.Parameters.Add(cmd, "@MinPlayers", DbType.Int32).Value = query.MinPlayers.Value;
            //}

            //if (query.MaxPlayers.HasValue)
            //{
            //    whereClauses.Add("Players<=@MaxPlayers");
            //    cmd.Parameters.Add(cmd, "@MaxPlayers", DbType.Int32).Value = query.MaxPlayers.Value;
            //}

            if (query.IndexNumber.HasValue)
                {
                    whereClauses.Add("IndexNumber = @IndexNumber");
                    cmd.Parameters.Add(cmd, "@IndexNumber", DbType.Int32).Value = query.IndexNumber.Value;
                }
            if (query.ParentIndexNumber.HasValue)
                {
                    whereClauses.Add("ParentIndexNumber = @ParentIndexNumber");
                    cmd.Parameters.Add(cmd, "@ParentIndexNumber", DbType.Int32).Value = query.ParentIndexNumber.Value;
                }
            if (query.ParentIndexNumberNotEquals.HasValue)
                {
                    whereClauses.Add("(ParentIndexNumber <> @ParentIndexNumberNotEquals OR ParentIndexNumber IS NULL)");
                    cmd.Parameters.Add(cmd, "@ParentIndexNumberNotEquals", DbType.Int32).Value = query.ParentIndexNumberNotEquals.Value;
                }
            if (query.MinEndDate.HasValue)
                {
                    whereClauses.Add("EndDate >= @MinEndDate");
                    cmd.Parameters.Add(cmd, "@MinEndDate", DbType.Date).Value = query.MinEndDate.Value;
                }

            if (query.MaxEndDate.HasValue)
                {
                    whereClauses.Add("EndDate <= @MaxEndDate");
                    cmd.Parameters.Add(cmd, "@MaxEndDate", DbType.Date).Value = query.MaxEndDate.Value;
                }

            if (query.MinStartDate.HasValue)
                {
                    whereClauses.Add("StartDate >= @MinStartDate");
                    cmd.Parameters.Add(cmd, "@MinStartDate", DbType.Date).Value = query.MinStartDate.Value;
                }

            if (query.MaxStartDate.HasValue)
                {
                    whereClauses.Add("StartDate <= @MaxStartDate");
                    cmd.Parameters.Add(cmd, "@MaxStartDate", DbType.Date).Value = query.MaxStartDate.Value;
                }

            if (query.MinPremiereDate.HasValue)
                {
                    whereClauses.Add("PremiereDate >= @MinPremiereDate");
                    cmd.Parameters.Add(cmd, "@MinPremiereDate", DbType.Date).Value = query.MinPremiereDate.Value;
                }
            if (query.MaxPremiereDate.HasValue)
                {
                    whereClauses.Add("PremiereDate <= @MaxPremiereDate");
                    cmd.Parameters.Add(cmd, "@MaxPremiereDate", DbType.Date).Value = query.MaxPremiereDate.Value;
                }

            if (query.SourceTypes.Length == 1)
                {
                    whereClauses.Add("SourceType = @SourceType");
                    cmd.Parameters.Add(cmd, "@SourceType", DbType.String).Value = query.SourceTypes[0];
                } else if (query.SourceTypes.Length > 1)
                {
                    var inClause = string.Join(",", query.SourceTypes.Select(i => "'" + i + "'").ToArray());
                    whereClauses.Add(string.Format("SourceType IN ({0})", inClause));
                }

            if (query.ExcludeSourceTypes.Length == 1)
                {
                    whereClauses.Add("SourceType <> @SourceType");
                    cmd.Parameters.Add(cmd, "@SourceType", DbType.String).Value = query.SourceTypes[0];
                } else if (query.ExcludeSourceTypes.Length > 1)
                {
                    var inClause = string.Join(",", query.ExcludeSourceTypes.Select(i => "'" + i + "'").ToArray());
                    whereClauses.Add(string.Format("SourceType NOT IN ({0})", inClause));
                }

            if (query.TrailerTypes.Length > 0)
                {
                    var clauses = new List<string>();
                    var index = 0;
                    foreach (var type in query.TrailerTypes)
                        {
                            clauses.Add("TrailerTypes LIKE @TrailerTypes" + index);
                            cmd.Parameters.Add(cmd, "@TrailerTypes" + index, DbType.String).Value = "%" + type + "%";
                            index++;
                        }
                    var clause = "(" + string.Join(" OR ", clauses.ToArray()) + ")";
                    whereClauses.Add(clause);
                }

            if (query.IsAiring.HasValue)
                {
                    if (query.IsAiring.Value)
                        {
                            whereClauses.Add("StartDate <= @MaxStartDate");
                            cmd.Parameters.Add(cmd, "@MaxStartDate", DbType.Date).Value = DateTime.UtcNow;

                            whereClauses.Add("EndDate >= @MinEndDate");
                            cmd.Parameters.Add(cmd, "@MinEndDate", DbType.Date).Value = DateTime.UtcNow;
                        } else
                        {
                            whereClauses.Add("(StartDate > @IsAiringDate OR EndDate < @IsAiringDate)");
                            cmd.Parameters.Add(cmd, "@IsAiringDate", DbType.Date).Value = DateTime.UtcNow;
                        }
                }

            if (query.PersonIds.Length > 0)
                {
                    // Todo: improve without having to do this
                    query.Person = query.PersonIds.Select(i => RetrieveItem(new Guid(i))).Where(i => i != null).Select(i => i.Name).FirstOrDefault();
                }

            if (!string.IsNullOrWhiteSpace(query.Person))
                {
                    whereClauses.Add("Guid IN (SELECT ItemId FROM People WHERE Name = @PersonName)");
                    cmd.Parameters.Add(cmd, "@PersonName", DbType.String).Value = query.Person;
                }

            if (!string.IsNullOrWhiteSpace(query.SlugName))
                {
                    whereClauses.Add("SlugName = @SlugName");
                    cmd.Parameters.Add(cmd, "@SlugName", DbType.String).Value = query.SlugName;
                }

            if (!string.IsNullOrWhiteSpace(query.MinSortName))
                {
                    whereClauses.Add("SortName >= @MinSortName");
                    cmd.Parameters.Add(cmd, "@MinSortName", DbType.String).Value = query.MinSortName;
                }

            if (!string.IsNullOrWhiteSpace(query.Name))
                {
                    whereClauses.Add("CleanName = @Name");
                    cmd.Parameters.Add(cmd, "@Name", DbType.String).Value = GetCleanValue(query.Name);
                }

            if (!string.IsNullOrWhiteSpace(query.NameContains))
                {
                    whereClauses.Add("CleanName LIKE @NameContains");
                    cmd.Parameters.Add(cmd, "@NameContains", DbType.String).Value = "%" + GetCleanValue(query.NameContains) + "%";
                }
            if (!string.IsNullOrWhiteSpace(query.NameStartsWith))
                {
                    whereClauses.Add("SortName LIKE @NameStartsWith");
                    cmd.Parameters.Add(cmd, "@NameStartsWith", DbType.String).Value = query.NameStartsWith + "%";
                }
            if (!string.IsNullOrWhiteSpace(query.NameStartsWithOrGreater))
                {
                    whereClauses.Add("SortName >= @NameStartsWithOrGreater");
                    // lowercase this because SortName is stored as lowercase
                    cmd.Parameters.Add(cmd, "@NameStartsWithOrGreater", DbType.String).Value = query.NameStartsWithOrGreater.ToLower();
                }
            if (!string.IsNullOrWhiteSpace(query.NameLessThan))
                {
                    whereClauses.Add("SortName < @NameLessThan");
                    // lowercase this because SortName is stored as lowercase
                    cmd.Parameters.Add(cmd, "@NameLessThan", DbType.String).Value = query.NameLessThan.ToLower();
                }

            if (query.ImageTypes.Length > 0 && _config.Configuration.SchemaVersion >= 87)
                {
                    var requiredImageIndex = 0;

                    foreach (var requiredImage in query.ImageTypes)
                        {
                            var paramName = "@RequiredImageType" + requiredImageIndex;
                            whereClauses.Add("(SELECT path FROM images WHERE ItemId = Guid AND ImageType = " + paramName + " LIMIT 1) NOT NULL");
                            cmd.Parameters.Add(cmd, paramName, DbType.Int32).Value = (int)requiredImage;
                            requiredImageIndex++;
                        }
                }

            if (query.IsLiked.HasValue)
                {
                    if (query.IsLiked.Value)
                        {
                            whereClauses.Add("rating >= @UserRating");
                            cmd.Parameters.Add(cmd, "@UserRating", DbType.Double).Value = UserItemData.MinLikeValue;
                        } else
                        {
                            whereClauses.Add("(rating IS NULL OR rating < @UserRating)");
                            cmd.Parameters.Add(cmd, "@UserRating", DbType.Double).Value = UserItemData.MinLikeValue;
                        }
                }

            if (query.IsFavoriteOrLiked.HasValue)
                {
                    if (query.IsFavoriteOrLiked.Value)
                        {
                            whereClauses.Add("IsFavorite = @IsFavoriteOrLiked");
                        } else
                        {
                            whereClauses.Add("(IsFavorite IS NULL OR IsFavorite = @IsFavoriteOrLiked)");
                        }
                    cmd.Parameters.Add(cmd, "@IsFavoriteOrLiked", DbType.Boolean).Value = query.IsFavoriteOrLiked.Value;
                }

            if (query.IsFavorite.HasValue)
                {
                    if (query.IsFavorite.Value)
                        {
                            whereClauses.Add("IsFavorite = @IsFavorite");
                        } else
                        {
                            whereClauses.Add("(IsFavorite IS NULL OR IsFavorite = @IsFavorite)");
                        }
                    cmd.Parameters.Add(cmd, "@IsFavorite", DbType.Boolean).Value = query.IsFavorite.Value;
                }

            if (EnableJoinUserData(query))
                {
                    if (query.IsPlayed.HasValue)
                        {
                            if (query.IsPlayed.Value)
                                {
                                    whereClauses.Add("(played = @IsPlayed)");
                                } else
                                {
                                    whereClauses.Add("(played IS NULL OR played = @IsPlayed)");
                                }
                            cmd.Parameters.Add(cmd, "@IsPlayed", DbType.Boolean).Value = query.IsPlayed.Value;
                        }
                }

            if (query.IsResumable.HasValue)
                {
                    if (query.IsResumable.Value)
                        {
                            whereClauses.Add("playbackPositionTicks > 0");
                        } else
                        {
                            whereClauses.Add("(playbackPositionTicks IS NULL OR playbackPositionTicks = 0)");
                        }
                }

            if (query.ArtistNames.Length > 0)
                {
                    var clauses = new List<string>();
                    var index = 0;
                    foreach (var artist in query.ArtistNames)
                        {
                            clauses.Add("@ArtistName" + index + " IN (SELECT CleanValue FROM ItemValues WHERE ItemId = Guid AND Type <= 1)");
                            cmd.Parameters.Add(cmd, "@ArtistName" + index, DbType.String).Value = GetCleanValue(artist);
                            index++;
                        }
                    var clause = "(" + string.Join(" OR ", clauses.ToArray()) + ")";
                    whereClauses.Add(clause);
                }

            if (query.ExcludeArtistIds.Length > 0)
                {
                    var clauses = new List<string>();
                    var index = 0;
                    foreach (var artistId in query.ExcludeArtistIds)
                        {
                            var artistItem = RetrieveItem(new Guid(artistId));
                            if (artistItem != null)
                                {
                                    clauses.Add("@ExcludeArtistName" + index + " NOT IN (SELECT CleanValue FROM ItemValues WHERE ItemId = Guid AND Type <= 1)");
                                    cmd.Parameters.Add(cmd, "@ExcludeArtistName" + index, DbType.String).Value = GetCleanValue(artistItem.Name);
                                    index++;
                                }
                        }
                    var clause = "(" + string.Join(" AND ", clauses.ToArray()) + ")";
                    whereClauses.Add(clause);
                }

            if (query.GenreIds.Length > 0)
                {
                    // Todo: improve without having to do this
                    query.Genres = query.GenreIds.Select(i => RetrieveItem(new Guid(i))).Where(i => i != null).Select(i => i.Name).ToArray();
                }

            if (query.Genres.Length > 0)
                {
                    var clauses = new List<string>();
                    var index = 0;
                    foreach (var item in query.Genres)
                        {
                            clauses.Add("@Genre" + index + " IN (SELECT CleanValue FROM ItemValues WHERE ItemId = Guid AND Type = 2)");
                            cmd.Parameters.Add(cmd, "@Genre" + index, DbType.String).Value = GetCleanValue(item);
                            index++;
                        }
                    var clause = "(" + string.Join(" OR ", clauses.ToArray()) + ")";
                    whereClauses.Add(clause);
                }

            if (query.Tags.Length > 0)
                {
                    var clauses = new List<string>();
                    var index = 0;
                    foreach (var item in query.Tags)
                        {
                            clauses.Add("@Tag" + index + " IN (SELECT CleanValue FROM ItemValues WHERE ItemId = Guid and Type = 4)");
                            cmd.Parameters.Add(cmd, "@Tag" + index, DbType.String).Value = GetCleanValue(item);
                            index++;
                        }
                    var clause = "(" + string.Join(" OR ", clauses.ToArray()) + ")";
                    whereClauses.Add(clause);
                }

            if (query.StudioIds.Length > 0)
                {
                    // Todo: improve without having to do this
                    query.Studios = query.StudioIds.Select(i => RetrieveItem(new Guid(i))).Where(i => i != null).Select(i => i.Name).ToArray();
                }

            if (query.Studios.Length > 0)
                {
                    var clauses = new List<string>();
                    var index = 0;
                    foreach (var item in query.Studios)
                        {
                            clauses.Add("@Studio" + index + " IN (SELECT CleanValue FROM ItemValues WHERE ItemId = Guid AND Type = 3)");
                            cmd.Parameters.Add(cmd, "@Studio" + index, DbType.String).Value = GetCleanValue(item);
                            index++;
                        }
                    var clause = "(" + string.Join(" OR ", clauses.ToArray()) + ")";
                    whereClauses.Add(clause);
                }

            if (query.Keywords.Length > 0)
                {
                    var clauses = new List<string>();
                    var index = 0;
                    foreach (var item in query.Keywords)
                        {
                            clauses.Add("@Keyword" + index + " IN (SELECT CleanValue FROM ItemValues WHERE ItemId = Guid AND Type = 5)");
                            cmd.Parameters.Add(cmd, "@Keyword" + index, DbType.String).Value = GetCleanValue(item);
                            index++;
                        }
                    var clause = "(" + string.Join(" OR ", clauses.ToArray()) + ")";
                    whereClauses.Add(clause);
                }

            if (query.OfficialRatings.Length > 0)
                {
                    var clauses = new List<string>();
                    var index = 0;
                    foreach (var item in query.OfficialRatings)
                        {
                            clauses.Add("OfficialRating = @OfficialRating" + index);
                            cmd.Parameters.Add(cmd, "@OfficialRating" + index, DbType.String).Value = item;
                            index++;
                        }
                    var clause = "(" + string.Join(" OR ", clauses.ToArray()) + ")";
                    whereClauses.Add(clause);
                }

            if (query.MinParentalRating.HasValue)
                {
                    whereClauses.Add("InheritedParentalRatingValue <= @MinParentalRating");
                    cmd.Parameters.Add(cmd, "@MinParentalRating", DbType.Int32).Value = query.MinParentalRating.Value;
                }

            if (query.MaxParentalRating.HasValue)
                {
                    whereClauses.Add("InheritedParentalRatingValue <= @MaxParentalRating");
                    cmd.Parameters.Add(cmd, "@MaxParentalRating", DbType.Int32).Value = query.MaxParentalRating.Value;
                }

            if (query.HasParentalRating.HasValue)
                {
                    if (query.HasParentalRating.Value)
                        {
                            whereClauses.Add("InheritedParentalRatingValue > 0");
                        } else
                        {
                            whereClauses.Add("InheritedParentalRatingValue = 0");
                        }
                }

            if (query.HasOverview.HasValue)
                {
                    if (query.HasOverview.Value)
                        {
                            whereClauses.Add("(Overview NOT NULL AND Overview <> '')");
                        } else
                        {
                            whereClauses.Add("(Overview IS NULL OR Overview = '')");
                        }
                }

            if (query.HasDeadParentId.HasValue)
                {
                    if (query.HasDeadParentId.Value)
                        {
                            whereClauses.Add("ParentId NOT NULL AND ParentId NOT IN (SELECT guid FROM TypedBaseItems)");
                        }
                }

            if (query.Years.Length == 1)
                {
                    whereClauses.Add("ProductionYear = @Years");
                    cmd.Parameters.Add(cmd, "@Years", DbType.Int32).Value = query.Years[0].ToString();
                } else if (query.Years.Length > 1)
                {
                    var val = string.Join(",", query.Years.ToArray());

                    whereClauses.Add("ProductionYear IN (" + val + ")");
                }

            if (query.LocationTypes.Length == 1)
                {
                    if (query.LocationTypes[0] == LocationType.Virtual && _config.Configuration.SchemaVersion >= 90)
                        {
                            query.IsVirtualItem = true;
                        } else
                        {
                            whereClauses.Add("LocationType = @LocationType");
                            cmd.Parameters.Add(cmd, "@LocationType", DbType.String).Value = query.LocationTypes[0].ToString();
                        }
                } else if (query.LocationTypes.Length > 1)
                {
                    var val = string.Join(",", query.LocationTypes.Select(i => "'" + i + "'").ToArray());

                    whereClauses.Add("LocationType IN (" + val + ")");
                }
            if (query.ExcludeLocationTypes.Length == 1)
                {
                    if (query.ExcludeLocationTypes[0] == LocationType.Virtual && _config.Configuration.SchemaVersion >= 90)
                        {
                            query.IsVirtualItem = false;
                        } else
                        {
                            whereClauses.Add("LocationType <> @ExcludeLocationTypes");
                            cmd.Parameters.Add(cmd, "@ExcludeLocationTypes", DbType.String).Value = query.ExcludeLocationTypes[0].ToString();
                        }
                } else if (query.ExcludeLocationTypes.Length > 1)
                {
                    var val = string.Join(",", query.ExcludeLocationTypes.Select(i => "'" + i + "'").ToArray());

                    whereClauses.Add("LocationType NOT IN (" + val + ")");
                }
            if (query.IsVirtualItem.HasValue)
                {
                    if (_config.Configuration.SchemaVersion >= 90)
                        {
                            whereClauses.Add("IsVirtualItem = @IsVirtualItem");
                            cmd.Parameters.Add(cmd, "@IsVirtualItem", DbType.Boolean).Value = query.IsVirtualItem.Value;
                        } else if (!query.IsVirtualItem.Value)
                        {
                            whereClauses.Add("LocationType <> 'Virtual'");
                        }
                }
            if (query.IsSpecialSeason.HasValue)
                {
                    if (query.IsSpecialSeason.Value)
                        {
                            whereClauses.Add("IndexNumber = 0");
                        } else
                        {
                            whereClauses.Add("IndexNumber <> 0");
                        }
                }
            if (query.IsUnaired.HasValue)
                {
                    if (query.IsUnaired.Value)
                        {
                            whereClauses.Add("PremiereDate >= DATETIME('now')");
                        } else
                        {
                            whereClauses.Add("PremiereDate < DATETIME('now')");
                        }
                }
            if (query.IsMissing.HasValue && _config.Configuration.SchemaVersion >= 90)
                {
                    if (query.IsMissing.Value)
                        {
                            whereClauses.Add("(IsVirtualItem = 1 AND PremiereDate < DATETIME('now'))");
                        } else
                        {
                            whereClauses.Add("(IsVirtualItem=0 OR PremiereDate >= DATETIME('now'))");
                        }
                }
            if (query.IsVirtualUnaired.HasValue && _config.Configuration.SchemaVersion >= 90)
                {
                    if (query.IsVirtualUnaired.Value)
                        {
                            whereClauses.Add("(IsVirtualItem = 1 AND PremiereDate >= DATETIME('now'))");
                        } else
                        {
                            whereClauses.Add("(IsVirtualItem = 0 OR PremiereDate < DATETIME('now'))");
                        }
                }
            if (query.MediaTypes.Length == 1)
                {
                    whereClauses.Add("MediaType = @MediaTypes");
                    cmd.Parameters.Add(cmd, "@MediaTypes", DbType.String).Value = query.MediaTypes[0];
                }
            if (query.MediaTypes.Length > 1)
                {
                    var val = string.Join(",", query.MediaTypes.Select(i => "'" + i + "'").ToArray());

                    whereClauses.Add("MediaType IN (" + val + ")");
                }
            if (query.ItemIds.Length > 0)
                {
                    var includeIds = new List<string>();

                    var index = 0;
                    foreach (var id in query.ItemIds)
                        {
                            includeIds.Add("Guid = @IncludeId" + index);
                            cmd.Parameters.Add(cmd, "@IncludeId" + index, DbType.Guid).Value = new Guid(id);
                            index++;
                        }

                    whereClauses.Add(string.Join(" OR ", includeIds.ToArray()));
                }
            if (query.ExcludeItemIds.Length > 0)
                {
                    var excludeIds = new List<string>();

                    var index = 0;
                    foreach (var id in query.ExcludeItemIds)
                        {
                            excludeIds.Add("Guid <> @ExcludeId" + index);
                            cmd.Parameters.Add(cmd, "@ExcludeId" + index, DbType.Guid).Value = new Guid(id);
                            index++;
                        }

                    whereClauses.Add(string.Join(" AND ", excludeIds.ToArray()));
                }

            if (query.ExcludeProviderIds.Count > 0)
                {
                    var excludeIds = new List<string>();

                    var index = 0;
                    foreach (var pair in query.ExcludeProviderIds)
                        {
                            if (string.Equals(pair.Key, MetadataProviders.TmdbCollection.ToString(), StringComparison.OrdinalIgnoreCase))
                                {
                                    continue;
                                }

                            var paramName = "@ExcludeProviderId" + index;
                            excludeIds.Add("(COALESCE((SELECT value FROM ProviderIds WHERE ItemId = Guid AND Name = '" + pair.Key + "'), '') <> " + paramName + ")");
                            cmd.Parameters.Add(cmd, paramName, DbType.String).Value = pair.Value;
                            index++;
                        }

                    whereClauses.Add(string.Join(" AND ", excludeIds.ToArray()));
                }

            if (query.HasImdbId.HasValue)
                {
                    var fn = query.HasImdbId.Value ? "<>" : "=";
                    whereClauses.Add("(COALESCE((SELECT value FROM ProviderIds WHERE ItemId = Guid AND Name = 'Imdb'), '') " + fn + " '')");
                }

            if (query.HasTmdbId.HasValue)
                {
                    var fn = query.HasTmdbId.Value ? "<>" : "=";
                    whereClauses.Add("(COALESCE((SELECT value FROM ProviderIds WHERE ItemId = Guid AND Name = 'Tmdb'), '') " + fn + " '')");
                }

            if (query.HasTvdbId.HasValue)
                {
                    var fn = query.HasTvdbId.Value ? "<>" : "=";
                    whereClauses.Add("(COALESCE((SELECT value FROM ProviderIds WHERE ItemId = Guid AND Name = 'Tvdb'), '') " + fn + " '')");
                }

            if (query.AlbumNames.Length > 0)
                {
                    var clause = "(";

                    var index = 0;
                    foreach (var name in query.AlbumNames)
                        {
                            if (index > 0)
                                {
                                    clause += " OR ";
                                }
                            clause += "Album = @AlbumName" + index;
                            cmd.Parameters.Add(cmd, "@AlbumName" + index, DbType.String).Value = name;
                            index++;
                        }

                    clause += ")";
                    whereClauses.Add(clause);
                }

            //var enableItemsByName = query.IncludeItemsByName ?? query.IncludeItemTypes.Length > 0;
            var enableItemsByName = query.IncludeItemsByName ?? false;

            if (query.TopParentIds.Length == 1)
                {
                    if (enableItemsByName)
                        {
                            whereClauses.Add("(TopParentId=@TopParentId OR IsItemByName=@IsItemByName)");
                            cmd.Parameters.Add(cmd, "@IsItemByName", DbType.Boolean).Value = true;
                        } else
                        {
                            whereClauses.Add("(TopParentId=@TopParentId)");
                        }
                    cmd.Parameters.Add(cmd, "@TopParentId", DbType.String).Value = query.TopParentIds[0];
                }
            if (query.TopParentIds.Length > 1)
                {
                    var val = string.Join(",", query.TopParentIds.Select(i => "'" + i + "'").ToArray());

                    if (enableItemsByName)
                        {
                            whereClauses.Add("(IsItemByName=@IsItemByName OR TopParentId IN (" + val + "))");
                            cmd.Parameters.Add(cmd, "@IsItemByName", DbType.Boolean).Value = true;
                        } else
                        {
                            whereClauses.Add("(TopParentId IN (" + val + "))");
                        }
                }

            if (query.AncestorIds.Length == 1)
                {
                    whereClauses.Add("Guid IN (SELECT ItemId FROM AncestorIds WHERE AncestorId = @AncestorId)");
                    cmd.Parameters.Add(cmd, "@AncestorId", DbType.Guid).Value = new Guid(query.AncestorIds[0]);
                }
            if (query.AncestorIds.Length > 1)
                {
                    var inClause = string.Join(",", query.AncestorIds.Select(i => "'" + new Guid(i).ToString("N") + "'").ToArray());
                    whereClauses.Add(string.Format("Guid IN (SELECT ItemId FROM AncestorIds WHERE AncestorIdText IN ({0}))", inClause));
                }
            if (!string.IsNullOrWhiteSpace(query.AncestorWithPresentationUniqueKey))
                {
                    var inClause = "SELECT guid FROM TypedBaseItems WHERE PresentationUniqueKey = @AncestorWithPresentationUniqueKey";
                    whereClauses.Add(string.Format("Guid IN (SELECT ItemId FROM AncestorIds WHERE AncestorId IN ({0}))", inClause));
                    cmd.Parameters.Add(cmd, "@AncestorWithPresentationUniqueKey", DbType.String).Value = query.AncestorWithPresentationUniqueKey;
                }

            if (query.BlockUnratedItems.Length == 1)
                {
                    whereClauses.Add("(InheritedParentalRatingValue > 0 OR UnratedType <> @UnratedType)");
                    cmd.Parameters.Add(cmd, "@UnratedType", DbType.String).Value = query.BlockUnratedItems[0].ToString();
                }
            if (query.BlockUnratedItems.Length > 1)
                {
                    var inClause = string.Join(",", query.BlockUnratedItems.Select(i => "'" + i.ToString() + "'").ToArray());
                    whereClauses.Add(string.Format("(InheritedParentalRatingValue > 0 OR UnratedType NOT IN ({0}))", inClause));
                }

            var excludeTagIndex = 0;
            foreach (var excludeTag in query.ExcludeTags)
                {
                    whereClauses.Add("(Tags IS NULL OR Tags NOT LIKE @excludeTag" + excludeTagIndex + ")");
                    cmd.Parameters.Add(cmd, "@excludeTag" + excludeTagIndex, DbType.String).Value = "%" + excludeTag + "%";
                    excludeTagIndex++;
                }

            excludeTagIndex = 0;
            foreach (var excludeTag in query.ExcludeInheritedTags)
                {
                    whereClauses.Add("(InheritedTags IS NULL OR InheritedTags NOT LIKE @excludeInheritedTag" + excludeTagIndex + ")");
                    cmd.Parameters.Add(cmd, "@excludeInheritedTag" + excludeTagIndex, DbType.String).Value = "%" + excludeTag + "%";
                    excludeTagIndex++;
                }

            return whereClauses;
        }

        private string GetCleanValue(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
                {
                    return value;
                }

            return value.RemoveDiacritics().ToLower();
        }

        private bool EnableGroupByPresentationUniqueKey(InternalItemsQuery query)
        {
            if (!query.GroupByPresentationUniqueKey)
                {
                    return false;
                }

            if (!string.IsNullOrWhiteSpace(query.PresentationUniqueKey))
                {
                    return false;
                }

            if (query.User == null)
                {
                    return false;
                }

            if (query.IncludeItemTypes.Length == 0)
                {
                    return true;
                }

            var types = new[] {
                typeof(Episode).Name,
                typeof(Video).Name,
                typeof(Movie).Name,
                typeof(MusicVideo).Name,
                typeof(Series).Name,
                typeof(Season).Name
            };

            if (types.Any(i => query.IncludeItemTypes.Contains(i, StringComparer.OrdinalIgnoreCase)))
                {
                    return true;
                }

            return false;
        }

        private static readonly Type[] KnownTypes = {
            typeof(LiveTvProgram),
            typeof(LiveTvChannel),
            typeof(LiveTvVideoRecording),
            typeof(LiveTvAudioRecording),
            typeof(Series),
            typeof(Audio),
            typeof(MusicAlbum),
            typeof(MusicArtist),
            typeof(MusicGenre),
            typeof(MusicVideo),
            typeof(Movie),
            typeof(Playlist),
            typeof(AudioPodcast),
            typeof(Trailer),
            typeof(BoxSet),
            typeof(Episode),
            typeof(Season),
            typeof(Series),
            typeof(Book),
            typeof(CollectionFolder),
            typeof(Folder),
            typeof(Game),
            typeof(GameGenre),
            typeof(GameSystem),
            typeof(Genre),
            typeof(Person),
            typeof(Photo),
            typeof(PhotoAlbum),
            typeof(Studio),
            typeof(UserRootFolder),
            typeof(UserView),
            typeof(Video),
            typeof(Year),
            typeof(Channel),
            typeof(AggregateFolder)
        };

        public async Task UpdateInheritedValues(CancellationToken cancellationToken)
        {
            await UpdateInheritedTags(cancellationToken).ConfigureAwait(false);
        }

        private async Task UpdateInheritedTags(CancellationToken cancellationToken)
        {
            var newValues = new List<Tuple<Guid, string>>();

            using (var cmd = _connection.CreateCommand())
                {
                    cmd.CommandText = "SELECT Guid, InheritedTags, (SELECT STRING_AGG(Tags, '|') FROM TypedBaseItems WHERE (guid = Outer.guid) OR (guid IN (Select AncestorId FROM AncestorIds WHERE ItemId = Outer.guid))) AS NewInheritedTags FROM TypedBaseItems AS Outer WHERE NewInheritedTags <> InheritedTags";

                    using (var reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult))
                        {
                            while (reader.Read())
                                {
                                    var id = reader.GetGuid(0);
                                    string value = reader.IsDBNull(2) ? null : reader.GetString(2);

                                    newValues.Add(new Tuple<Guid, string>(id, value));
                                }
                        }
                }

            Logger.Debug("UpdateInheritedTags - {0} rows", newValues.Count);
            if (newValues.Count == 0)
                {
                    return;
                }

            await WriteLock.WaitAsync(cancellationToken).ConfigureAwait(false);

            IDbTransaction transaction = null;

            try
                {
                    transaction = _connection.BeginTransaction();

                    foreach (var item in newValues)
                        {
                            _updateInheritedTagsCommand.GetParameter(0).Value = item.Item1;
                            _updateInheritedTagsCommand.GetParameter(1).Value = item.Item2;

                            _updateInheritedTagsCommand.Transaction = transaction;
                            _updateInheritedTagsCommand.ExecuteNonQuery();
                        }

                    transaction.Commit();
                } catch (OperationCanceledException)
                {
                    if (transaction != null)
                        {
                            transaction.Rollback();
                        }

                    throw;
                } catch (Exception e)
                {
                    Logger.ErrorException("Error running query:", e);

                    if (transaction != null)
                        {
                            transaction.Rollback();
                        }

                    throw;
                } finally
                {
                    if (transaction != null)
                        {
                            transaction.Dispose();
                        }

                    WriteLock.Release();
                }
        }

        private static Dictionary<string, string[]> GetTypeMapDictionary()
        {
            var dict = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);

            foreach (var t in KnownTypes)
                {
                    dict[t.Name] = new[] { t.FullName };
                }

            dict["Recording"] = new[] {
                typeof(LiveTvAudioRecording).FullName,
                typeof(LiveTvVideoRecording).FullName
            };
            dict["Program"] = new[] { typeof(LiveTvProgram).FullName };
            dict["TvChannel"] = new[] { typeof(LiveTvChannel).FullName };

            return dict;
        }

        // Not crazy about having this all the way down here, but at least it's in one place
        readonly Dictionary<string, string[]> _types = GetTypeMapDictionary();

        private IEnumerable<string> MapIncludeItemTypes(string value)
        {
            string[] result;
            if (_types.TryGetValue(value, out result))
                {
                    return result;
                }

            return new[] { value };
        }

        public async Task DeleteItem(Guid id, CancellationToken cancellationToken)
        {
            if (id == Guid.Empty)
                {
                    throw new ArgumentNullException("id");
                }

            CheckDisposed();

            await WriteLock.WaitAsync(cancellationToken).ConfigureAwait(false);

            IDbTransaction transaction = null;

            try
                {
                    transaction = _connection.BeginTransaction();

                    // Delete people
                    _deletePeopleCommand.GetParameter(0).Value = id;
                    _deletePeopleCommand.Transaction = transaction;
                    _deletePeopleCommand.ExecuteNonQuery();

                    // Delete chapters
                    _deleteChaptersCommand.GetParameter(0).Value = id;
                    _deleteChaptersCommand.Transaction = transaction;
                    _deleteChaptersCommand.ExecuteNonQuery();

                    // Delete media streams
                    _deleteStreamsCommand.GetParameter(0).Value = id;
                    _deleteStreamsCommand.Transaction = transaction;
                    _deleteStreamsCommand.ExecuteNonQuery();

                    // Delete ancestors
                    _deleteAncestorsCommand.GetParameter(0).Value = id;
                    _deleteAncestorsCommand.Transaction = transaction;
                    _deleteAncestorsCommand.ExecuteNonQuery();

                    // Delete user data keys
                    _deleteUserDataKeysCommand.GetParameter(0).Value = id;
                    _deleteUserDataKeysCommand.Transaction = transaction;
                    _deleteUserDataKeysCommand.ExecuteNonQuery();

                    // Delete item values
                    _deleteItemValuesCommand.GetParameter(0).Value = id;
                    _deleteItemValuesCommand.Transaction = transaction;
                    _deleteItemValuesCommand.ExecuteNonQuery();

                    // Delete provider ids
                    _deleteProviderIdsCommand.GetParameter(0).Value = id;
                    _deleteProviderIdsCommand.Transaction = transaction;
                    _deleteProviderIdsCommand.ExecuteNonQuery();

                    // Delete images
                    _deleteImagesCommand.GetParameter(0).Value = id;
                    _deleteImagesCommand.Transaction = transaction;
                    _deleteImagesCommand.ExecuteNonQuery();

                    // Delete the item
                    _deleteItemCommand.GetParameter(0).Value = id;
                    _deleteItemCommand.Transaction = transaction;
                    _deleteItemCommand.ExecuteNonQuery();

                    transaction.Commit();
                } catch (OperationCanceledException)
                {
                    if (transaction != null)
                        {
                            transaction.Rollback();
                        }

                    throw;
                } catch (Exception e)
                {
                    Logger.ErrorException("Failed to save children:", e);

                    if (transaction != null)
                        {
                            transaction.Rollback();
                        }

                    throw;
                } finally
                {
                    if (transaction != null)
                        {
                            transaction.Dispose();
                        }

                    WriteLock.Release();
                }
        }

        public List<string> GetPeopleNames(InternalPeopleQuery query)
        {
            if (query == null)
                {
                    throw new ArgumentNullException("query");
                }

            CheckDisposed();

            using (var cmd = _connection.CreateCommand())
                {
                    cmd.CommandText = "SELECT DISTINCT Name FROM People";

                    var whereClauses = GetPeopleWhereClauses(query, cmd);

                    if (whereClauses.Count > 0)
                        {
                            cmd.CommandText += "  WHERE " + string.Join(" AND ", whereClauses.ToArray());
                        }

                    cmd.CommandText += " ORDER BY ListOrder";

                    var list = new List<string>();

                    using (var reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult))
                        {
                            while (reader.Read())
                                {
                                    list.Add(reader.GetString(0));
                                }
                        }

                    return list;
                }
        }

        public List<PersonInfo> GetPeople(InternalPeopleQuery query)
        {
            if (query == null)
                {
                    throw new ArgumentNullException("query");
                }

            CheckDisposed();

            using (var cmd = _connection.CreateCommand())
                {
                    cmd.CommandText = "SELECT ItemId, Name, Role, PersonType, SortOrder FROM People";

                    var whereClauses = GetPeopleWhereClauses(query, cmd);

                    if (whereClauses.Count > 0)
                        {
                            cmd.CommandText += "  where " + string.Join(" AND ", whereClauses.ToArray());
                        }

                    cmd.CommandText += " ORDER BY ListOrder";

                    var list = new List<PersonInfo>();

                    using (var reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult))
                        {
                            while (reader.Read())
                                {
                                    list.Add(GetPerson(reader));
                                }
                        }

                    return list;
                }
        }

        private List<string> GetPeopleWhereClauses(InternalPeopleQuery query, IDbCommand cmd)
        {
            var whereClauses = new List<string>();

            if (query.ItemId != Guid.Empty)
                {
                    whereClauses.Add("ItemId = @ItemId");
                    cmd.Parameters.Add(cmd, "@ItemId", DbType.Guid).Value = query.ItemId;
                }
            if (query.AppearsInItemId != Guid.Empty)
                {
                    whereClauses.Add("Name IN (SELECT Name FROM People WHERE ItemId = @AppearsInItemId)");
                    cmd.Parameters.Add(cmd, "@AppearsInItemId", DbType.Guid).Value = query.AppearsInItemId;
                }
            if (query.PersonTypes.Count == 1)
                {
                    whereClauses.Add("PersonType = @PersonType");
                    cmd.Parameters.Add(cmd, "@PersonType", DbType.String).Value = query.PersonTypes[0];
                }
            if (query.PersonTypes.Count > 1)
                {
                    var val = string.Join(",", query.PersonTypes.Select(i => "'" + i + "'").ToArray());

                    whereClauses.Add("PersonType IN (" + val + ")");
                }
            if (query.ExcludePersonTypes.Count == 1)
                {
                    whereClauses.Add("PersonType <> @PersonType");
                    cmd.Parameters.Add(cmd, "@PersonType", DbType.String).Value = query.ExcludePersonTypes[0];
                }
            if (query.ExcludePersonTypes.Count > 1)
                {
                    var val = string.Join(",", query.ExcludePersonTypes.Select(i => "'" + i + "'").ToArray());

                    whereClauses.Add("PersonType NOT IN (" + val + ")");
                }
            if (query.MaxListOrder.HasValue)
                {
                    whereClauses.Add("ListOrder <= @MaxListOrder");
                    cmd.Parameters.Add(cmd, "@MaxListOrder", DbType.Int32).Value = query.MaxListOrder.Value;
                }
            if (!string.IsNullOrWhiteSpace(query.NameContains))
                {
                    whereClauses.Add("Name LIKE @NameContains");
                    cmd.Parameters.Add(cmd, "@NameContains", DbType.String).Value = "%" + query.NameContains + "%";
                }
            if (query.SourceTypes.Length == 1)
                {
                    whereClauses.Add("(SELECT SourceType FROM TypedBaseItems WHERE guid = ItemId) = @SourceTypes");
                    cmd.Parameters.Add(cmd, "@SourceTypes", DbType.String).Value = query.SourceTypes[0].ToString();
                }

            return whereClauses;
        }

        private void UpdateAncestors(Guid itemId, List<Guid> ancestorIds, IDbTransaction transaction)
        {
            if (itemId == Guid.Empty)
                {
                    throw new ArgumentNullException("itemId");
                }

            if (ancestorIds == null)
                {
                    throw new ArgumentNullException("ancestorIds");
                }

            CheckDisposed();

            // First delete 
            _deleteAncestorsCommand.GetParameter(0).Value = itemId;
            _deleteAncestorsCommand.Transaction = transaction;

            _deleteAncestorsCommand.ExecuteNonQuery();

            foreach (var ancestorId in ancestorIds)
                {
                    _saveAncestorCommand.GetParameter(0).Value = itemId;
                    _saveAncestorCommand.GetParameter(1).Value = ancestorId;
                    _saveAncestorCommand.GetParameter(2).Value = ancestorId.ToString("N");

                    _saveAncestorCommand.Transaction = transaction;

                    _saveAncestorCommand.ExecuteNonQuery();
                }
        }

        public QueryResult<Tuple<BaseItem, ItemCounts>> GetAllArtists(InternalItemsQuery query)
        {
            return GetItemValues(query, new[] { 0, 1 }, typeof(MusicArtist).FullName);
        }

        public QueryResult<Tuple<BaseItem, ItemCounts>> GetArtists(InternalItemsQuery query)
        {
            return GetItemValues(query, new[] { 0 }, typeof(MusicArtist).FullName);
        }

        public QueryResult<Tuple<BaseItem, ItemCounts>> GetAlbumArtists(InternalItemsQuery query)
        {
            return GetItemValues(query, new[] { 1 }, typeof(MusicArtist).FullName);
        }

        public QueryResult<Tuple<BaseItem, ItemCounts>> GetStudios(InternalItemsQuery query)
        {
            return GetItemValues(query, new[] { 3 }, typeof(Studio).FullName);
        }

        public QueryResult<Tuple<BaseItem, ItemCounts>> GetGenres(InternalItemsQuery query)
        {
            return GetItemValues(query, new[] { 2 }, typeof(Genre).FullName);
        }

        public QueryResult<Tuple<BaseItem, ItemCounts>> GetGameGenres(InternalItemsQuery query)
        {
            return GetItemValues(query, new[] { 2 }, typeof(GameGenre).FullName);
        }

        public QueryResult<Tuple<BaseItem, ItemCounts>> GetMusicGenres(InternalItemsQuery query)
        {
            return GetItemValues(query, new[] { 2 }, typeof(MusicGenre).FullName);
        }

        public List<string> GetStudioNames()
        {
            return GetItemValueNames(new[] { 3 }, new List<string>(), new List<string>());
        }

        public List<string> GetAllArtistNames()
        {
            return GetItemValueNames(new[] { 0, 1 }, new List<string>(), new List<string>());
        }

        public List<string> GetMusicGenreNames()
        {
            return GetItemValueNames(new[] { 2 }, new List<string> {
                "Audio",
                "MusicVideo",
                "MusicAlbum",
                "MusicArtist"
            }, new List<string>());
        }

        public List<string> GetGameGenreNames()
        {
            return GetItemValueNames(new[] { 2 }, new List<string> { "Game" }, new List<string>());
        }

        public List<string> GetGenreNames()
        {
            return GetItemValueNames(new[] { 2 }, new List<string>(), new List<string> {
                "Audio",
                "MusicVideo",
                "MusicAlbum",
                "MusicArtist",
                "Game",
                "GameSystem"
            });
        }

        private List<string> GetItemValueNames(int[] itemValueTypes, List<string> withItemTypes, List<string> excludeItemTypes)
        {
            CheckDisposed();

            withItemTypes = withItemTypes.SelectMany(MapIncludeItemTypes).ToList();
            excludeItemTypes = excludeItemTypes.SelectMany(MapIncludeItemTypes).ToList();

            var now = DateTime.UtcNow;

            var typeClause = itemValueTypes.Length == 1 ?
                ("Type = " + itemValueTypes[0].ToString(CultureInfo.InvariantCulture)) :
                ("Type IN (" + string.Join(",", itemValueTypes.Select(i => i.ToString(CultureInfo.InvariantCulture)).ToArray()) + ")");

            var list = new List<string>();

            using (var cmd = _connection.CreateCommand())
                {
                    cmd.CommandText = "SELECT Value FROM ItemValues WHERE " + typeClause;

                    if (withItemTypes.Count > 0)
                        {
                            var typeString = string.Join(",", withItemTypes.Select(i => "'" + i + "'").ToArray());
                            cmd.CommandText += " AND ItemId IN (SELECT guid FROM TypedBaseItems WHERE type IN (" + typeString + "))";
                        }
                    if (excludeItemTypes.Count > 0)
                        {
                            var typeString = string.Join(",", excludeItemTypes.Select(i => "'" + i + "'").ToArray());
                            cmd.CommandText += " AND ItemId NOT IN (SELECT guid FROM TypedBaseItems WHERE type IN (" + typeString + "))";
                        }

                    cmd.CommandText += " GROUP BY CleanValue";

                    var commandBehavior = CommandBehavior.SequentialAccess | CommandBehavior.SingleResult;

                    using (var reader = cmd.ExecuteReader(commandBehavior))
                        {
                            LogQueryTime("GetItemValueNames", cmd, now);

                            while (reader.Read())
                                {
                                    if (!reader.IsDBNull(0))
                                        {
                                            list.Add(reader.GetString(0));
                                        }
                                }
                        }

                }

            return list;
        }

        private QueryResult<Tuple<BaseItem, ItemCounts>> GetItemValues(InternalItemsQuery query, int[] itemValueTypes, string returnType)
        {
            if (query == null)
                {
                    throw new ArgumentNullException("query");
                }

            if (!query.Limit.HasValue)
                {
                    query.EnableTotalRecordCount = false;
                }

            CheckDisposed();

            var now = DateTime.UtcNow;

            var typeClause = itemValueTypes.Length == 1 ?
                ("Type = " + itemValueTypes[0].ToString(CultureInfo.InvariantCulture)) :
                ("Type IN (" + string.Join(",", itemValueTypes.Select(i => i.ToString(CultureInfo.InvariantCulture)).ToArray()) + ")");

            using (var cmd = _connection.CreateCommand())
                {
                    var itemCountColumns = new List<Tuple<string, string>>();

                    var typesToCount = query.IncludeItemTypes.ToList();

                    if (typesToCount.Count > 0)
                        {
                            var itemCountColumnQuery = "SELECT STRING_AGG(type, '|')" + GetFromText("B");

                            var typeSubQuery = new InternalItemsQuery(query.User) {
                                ExcludeItemTypes = query.ExcludeItemTypes,
                                IncludeItemTypes = query.IncludeItemTypes,
                                MediaTypes = query.MediaTypes,
                                AncestorIds = query.AncestorIds,
                                ExcludeItemIds = query.ExcludeItemIds,
                                ItemIds = query.ItemIds,
                                TopParentIds = query.TopParentIds,
                                ParentId = query.ParentId,
                                IsPlayed = query.IsPlayed
                            };
                            var whereClauses = GetWhereClauses(typeSubQuery, cmd, "itemTypes");

                            whereClauses.Add("guid IN (SELECT ItemId FROM ItemValues WHERE ItemValues.CleanValue = A.CleanName AND " + typeClause + ")");

                            var typeWhereText = whereClauses.Count == 0 ?
                        string.Empty :
                        " WHERE " + string.Join(" AND ", whereClauses.ToArray());

                            itemCountColumnQuery += typeWhereText;

                            //itemCountColumnQuery += ")";

                            itemCountColumns.Add(new Tuple<string, string>("itemTypes", "(" + itemCountColumnQuery + ") AS itemTypes"));
                        }

                    var columns = _retriveItemColumns.ToList();
                    columns.AddRange(itemCountColumns.Select(i => i.Item2).ToArray());

                    cmd.CommandText = "SELECT " + string.Join(",", GetFinalColumnsToSelect(query, columns.ToArray(), cmd)) + GetFromText();
                    cmd.CommandText += GetJoinUserDataText(query);

                    var innerQuery = new InternalItemsQuery(query.User) {
                        ExcludeItemTypes = query.ExcludeItemTypes,
                        IncludeItemTypes = query.IncludeItemTypes,
                        MediaTypes = query.MediaTypes,
                        AncestorIds = query.AncestorIds,
                        ExcludeItemIds = query.ExcludeItemIds,
                        ItemIds = query.ItemIds,
                        TopParentIds = query.TopParentIds,
                        ParentId = query.ParentId,
                        IsPlayed = query.IsPlayed
                    };

                    var innerWhereClauses = GetWhereClauses(innerQuery, cmd);

                    var innerWhereText = innerWhereClauses.Count == 0 ?
                    string.Empty :
                    " WHERE " + string.Join(" AND ", innerWhereClauses.ToArray());

                    var whereText = " WHERE Type = @SelectType";

                    if (typesToCount.Count == 0)
                        {
                            whereText += " AND CleanName IN (SELECT CleanValue FROM ItemValues WHERE " + typeClause + " AND ItemId IN (SELECT guid FROM TypedBaseItems" + innerWhereText + "))";
                        } else
                        {
                            //whereText += " And itemTypes not null";
                            whereText += " AND CleanName IN (SELECT CleanValue FROM ItemValues WHERE " + typeClause + " AND ItemId IN (SELECT guid FROM TypedBaseItems" + innerWhereText + "))";
                        }

                    var outerQuery = new InternalItemsQuery(query.User) {
                        IsFavorite = query.IsFavorite,
                        IsFavoriteOrLiked = query.IsFavoriteOrLiked,
                        IsLiked = query.IsLiked,
                        IsLocked = query.IsLocked,
                        NameLessThan = query.NameLessThan,
                        NameStartsWith = query.NameStartsWith,
                        NameStartsWithOrGreater = query.NameStartsWithOrGreater,
                        AlbumArtistStartsWithOrGreater = query.AlbumArtistStartsWithOrGreater,
                        Tags = query.Tags,
                        OfficialRatings = query.OfficialRatings,
                        GenreIds = query.GenreIds,
                        Genres = query.Genres,
                        Years = query.Years
                    };

                    var outerWhereClauses = GetWhereClauses(outerQuery, cmd);

                    whereText += outerWhereClauses.Count == 0 ?
                    string.Empty :
                    " AND " + string.Join(" AND ", outerWhereClauses.ToArray());
                    //cmd.CommandText += GetGroupBy(query);

                    cmd.CommandText += whereText;
                    cmd.CommandText += " GROUP BY PresentationUniqueKey";

                    cmd.Parameters.Add(cmd, "@SelectType", DbType.String).Value = returnType;

                    if (EnableJoinUserData(query))
                        {
                            cmd.Parameters.Add(cmd, "@UserId", DbType.Guid).Value = query.User.Id;
                        }

                    cmd.CommandText += " ORDER BY SortName";

                    if (query.Limit.HasValue || query.StartIndex.HasValue)
                        {
                            var offset = query.StartIndex ?? 0;

                            if (query.Limit.HasValue || offset > 0)
                                {
                                    cmd.CommandText += " LIMIT " + (query.Limit ?? int.MaxValue).ToString(CultureInfo.InvariantCulture);
                                }

                            if (offset > 0)
                                {
                                    cmd.CommandText += " OFFSET " + offset.ToString(CultureInfo.InvariantCulture);
                                }
                        }

                    cmd.CommandText += ";";

                    var isReturningZeroItems = query.Limit.HasValue && query.Limit <= 0;

                    if (isReturningZeroItems)
                        {
                            cmd.CommandText = "";
                        }

                    if (query.EnableTotalRecordCount)
                        {
                            cmd.CommandText += "SELECT COUNT(DISTINCT PresentationUniqueKey)" + GetFromText();

                            cmd.CommandText += GetJoinUserDataText(query);
                            cmd.CommandText += whereText;
                        } else
                        {
                            cmd.CommandText = cmd.CommandText.TrimEnd(';');
                        }

                    var list = new List<Tuple<BaseItem, ItemCounts>>();
                    var count = 0;

                    var commandBehavior = isReturningZeroItems || !query.EnableTotalRecordCount
                    ? (CommandBehavior.SequentialAccess | CommandBehavior.SingleResult)
                    : CommandBehavior.SequentialAccess;

                    //Logger.Debug("GetItemValues: " + cmd.CommandText);

                    using (var reader = cmd.ExecuteReader(commandBehavior))
                        {
                            LogQueryTime("GetItemValues", cmd, now);

                            if (isReturningZeroItems)
                                {
                                    if (reader.Read())
                                        {
                                            count = reader.GetInt32(0);
                                        }
                                } else
                                {
                                    while (reader.Read())
                                        {
                                            var item = GetItem(reader);
                                            if (item != null)
                                                {
                                                    var countStartColumn = columns.Count - 1;

                                                    list.Add(new Tuple<BaseItem, ItemCounts>(item, GetItemCounts(reader, countStartColumn, typesToCount)));
                                                }
                                        }

                                    if (reader.NextResult() && reader.Read())
                                        {
                                            count = reader.GetInt32(0);
                                        }
                                }
                        }

                    if (count == 0)
                        {
                            count = list.Count;
                        }

                    return new QueryResult<Tuple<BaseItem, ItemCounts>> {
                        Items = list.ToArray(),
                        TotalRecordCount = count
                    };

                }
        }

        private ItemCounts GetItemCounts(IDataReader reader, int countStartColumn, List<string> typesToCount)
        {
            var counts = new ItemCounts();

            if (typesToCount.Count == 0)
                {
                    return counts;
                }

            var typeString = reader.IsDBNull(countStartColumn) ? null : reader.GetString(countStartColumn);

            if (string.IsNullOrWhiteSpace(typeString))
                {
                    return counts;
                }

            var allTypes = typeString.Split(new[] { '|' }, StringSplitOptions.RemoveEmptyEntries)
                .ToLookup(i => i).ToList();

            foreach (var type in allTypes)
                {
                    var value = type.ToList().Count;
                    var typeName = type.Key;

                    if (string.Equals(typeName, typeof(Series).FullName, StringComparison.OrdinalIgnoreCase))
                        {
                            counts.SeriesCount = value;
                        } else if (string.Equals(typeName, typeof(Episode).FullName, StringComparison.OrdinalIgnoreCase))
                        {
                            counts.EpisodeCount = value;
                        } else if (string.Equals(typeName, typeof(Movie).FullName, StringComparison.OrdinalIgnoreCase))
                        {
                            counts.MovieCount = value;
                        } else if (string.Equals(typeName, typeof(MusicAlbum).FullName, StringComparison.OrdinalIgnoreCase))
                        {
                            counts.AlbumCount = value;
                        } else if (string.Equals(typeName, typeof(MusicArtist).FullName, StringComparison.OrdinalIgnoreCase))
                        {
                            counts.ArtistCount = value;
                        } else if (string.Equals(typeName, typeof(Audio).FullName, StringComparison.OrdinalIgnoreCase))
                        {
                            counts.SongCount = value;
                        } else if (string.Equals(typeName, typeof(Game).FullName, StringComparison.OrdinalIgnoreCase))
                        {
                            counts.GameCount = value;
                        } else if (string.Equals(typeName, typeof(Trailer).FullName, StringComparison.OrdinalIgnoreCase))
                        {
                            counts.TrailerCount = value;
                        }
                    counts.ItemCount += value;
                }

            return counts;
        }

        private List<Tuple<int, string>> GetItemValuesToSave(BaseItem item)
        {
            var list = new List<Tuple<int, string>>();

            var hasArtist = item as IHasArtist;
            if (hasArtist != null)
                {
                    list.AddRange(hasArtist.Artists.Select(i => new Tuple<int, string>(0, i)));
                }

            var hasAlbumArtist = item as IHasAlbumArtist;
            if (hasAlbumArtist != null)
                {
                    list.AddRange(hasAlbumArtist.AlbumArtists.Select(i => new Tuple<int, string>(1, i)));
                }

            list.AddRange(item.Genres.Select(i => new Tuple<int, string>(2, i)));
            list.AddRange(item.Studios.Select(i => new Tuple<int, string>(3, i)));
            list.AddRange(item.Tags.Select(i => new Tuple<int, string>(4, i)));
            list.AddRange(item.Keywords.Select(i => new Tuple<int, string>(5, i)));

            return list;
        }

        private void UpdateImages(Guid itemId, List<ItemImageInfo> images, IDbTransaction transaction)
        {
            if (itemId == Guid.Empty)
                {
                    throw new ArgumentNullException("itemId");
                }

            if (images == null)
                {
                    throw new ArgumentNullException("images");
                }

            CheckDisposed();

            // First delete 
            _deleteImagesCommand.GetParameter(0).Value = itemId;
            _deleteImagesCommand.Transaction = transaction;

            _deleteImagesCommand.ExecuteNonQuery();

            var index = 0;
            foreach (var image in images)
                {
                    if (string.IsNullOrWhiteSpace(image.Path))
                        {
                            // Invalid
                            continue;
                        }

                    _saveImagesCommand.GetParameter(0).Value = itemId;
                    _saveImagesCommand.GetParameter(1).Value = image.Type;
                    _saveImagesCommand.GetParameter(2).Value = image.Path;

                    if (image.DateModified == default(DateTime))
                        {
                            _saveImagesCommand.GetParameter(3).Value = null;
                        } else
                        {
                            _saveImagesCommand.GetParameter(3).Value = image.DateModified;
                        }

                    _saveImagesCommand.GetParameter(4).Value = image.IsPlaceholder;
                    _saveImagesCommand.GetParameter(5).Value = index;

                    _saveImagesCommand.Transaction = transaction;

                    _saveImagesCommand.ExecuteNonQuery();
                    index++;
                }
        }

        private void UpdateProviderIds(Guid itemId, Dictionary<string, string> values, IDbTransaction transaction)
        {
            if (itemId == Guid.Empty)
                {
                    throw new ArgumentNullException("itemId");
                }

            if (values == null)
                {
                    throw new ArgumentNullException("values");
                }

            // Just in case there might be case-insensitive duplicates, strip them out now
            var newValues = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var pair in values)
                {
                    newValues[pair.Key] = pair.Value;
                }

            CheckDisposed();

            // First delete 
            _deleteProviderIdsCommand.GetParameter(0).Value = itemId;
            _deleteProviderIdsCommand.Transaction = transaction;

            _deleteProviderIdsCommand.ExecuteNonQuery();

            foreach (var pair in newValues)
                {
                    _saveProviderIdsCommand.GetParameter(0).Value = itemId;
                    _saveProviderIdsCommand.GetParameter(1).Value = pair.Key;
                    _saveProviderIdsCommand.GetParameter(2).Value = pair.Value;
                    _saveProviderIdsCommand.Transaction = transaction;

                    _saveProviderIdsCommand.ExecuteNonQuery();
                }
        }

        private void UpdateItemValues(Guid itemId, List<Tuple<int, string>> values, IDbTransaction transaction)
        {
            if (itemId == Guid.Empty)
                {
                    throw new ArgumentNullException("itemId");
                }

            if (values == null)
                {
                    throw new ArgumentNullException("keys");
                }

            CheckDisposed();

            // First delete 
            _deleteItemValuesCommand.GetParameter(0).Value = itemId;
            _deleteItemValuesCommand.Transaction = transaction;

            _deleteItemValuesCommand.ExecuteNonQuery();

            foreach (var pair in values)
                {
                    _saveItemValuesCommand.GetParameter(0).Value = itemId;
                    _saveItemValuesCommand.GetParameter(1).Value = pair.Item1;
                    _saveItemValuesCommand.GetParameter(2).Value = pair.Item2;
                    if (pair.Item2 == null)
                        {
                            _saveItemValuesCommand.GetParameter(3).Value = null;
                        } else
                        {
                            _saveItemValuesCommand.GetParameter(3).Value = GetCleanValue(pair.Item2);
                        }
                    _saveItemValuesCommand.Transaction = transaction;

                    _saveItemValuesCommand.ExecuteNonQuery();
                }
        }

        private void UpdateUserDataKeys(Guid itemId, List<string> keys, IDbTransaction transaction)
        {
            if (itemId == Guid.Empty)
                {
                    throw new ArgumentNullException("itemId");
                }

            if (keys == null)
                {
                    throw new ArgumentNullException("keys");
                }

            CheckDisposed();

            // First delete 
            _deleteUserDataKeysCommand.GetParameter(0).Value = itemId;
            _deleteUserDataKeysCommand.Transaction = transaction;

            _deleteUserDataKeysCommand.ExecuteNonQuery();
            var index = 0;

            foreach (var key in keys)
                {
                    _saveUserDataKeysCommand.GetParameter(0).Value = itemId;
                    _saveUserDataKeysCommand.GetParameter(1).Value = key;
                    _saveUserDataKeysCommand.GetParameter(2).Value = index;
                    index++;
                    _saveUserDataKeysCommand.Transaction = transaction;

                    _saveUserDataKeysCommand.ExecuteNonQuery();
                }
        }

        public async Task UpdatePeople(Guid itemId, List<PersonInfo> people)
        {
            if (itemId == Guid.Empty)
                {
                    throw new ArgumentNullException("itemId");
                }

            if (people == null)
                {
                    throw new ArgumentNullException("people");
                }

            CheckDisposed();

            var cancellationToken = CancellationToken.None;

            await WriteLock.WaitAsync(cancellationToken).ConfigureAwait(false);

            IDbTransaction transaction = null;

            try
                {
                    transaction = _connection.BeginTransaction();

                    // First delete 
                    _deletePeopleCommand.GetParameter(0).Value = itemId;
                    _deletePeopleCommand.Transaction = transaction;

                    _deletePeopleCommand.ExecuteNonQuery();

                    var listIndex = 0;

                    foreach (var person in people)
                        {
                            cancellationToken.ThrowIfCancellationRequested();

                            _savePersonCommand.GetParameter(0).Value = itemId;
                            _savePersonCommand.GetParameter(1).Value = person.Name;
                            _savePersonCommand.GetParameter(2).Value = person.Role;
                            _savePersonCommand.GetParameter(3).Value = person.Type;
                            _savePersonCommand.GetParameter(4).Value = person.SortOrder;
                            _savePersonCommand.GetParameter(5).Value = listIndex;

                            _savePersonCommand.Transaction = transaction;

                            _savePersonCommand.ExecuteNonQuery();
                            listIndex++;
                        }

                    transaction.Commit();
                } catch (OperationCanceledException)
                {
                    if (transaction != null)
                        {
                            transaction.Rollback();
                        }

                    throw;
                } catch (Exception e)
                {
                    Logger.ErrorException("Failed to save people:", e);

                    if (transaction != null)
                        {
                            transaction.Rollback();
                        }

                    throw;
                } finally
                {
                    if (transaction != null)
                        {
                            transaction.Dispose();
                        }

                    WriteLock.Release();
                }
        }

        private PersonInfo GetPerson(IDataReader reader)
        {
            var item = new PersonInfo();

            item.ItemId = reader.GetGuid(0);
            item.Name = reader.GetString(1);

            if (!reader.IsDBNull(2))
                {
                    item.Role = reader.GetString(2);
                }

            if (!reader.IsDBNull(3))
                {
                    item.Type = reader.GetString(3);
                }

            if (!reader.IsDBNull(4))
                {
                    item.SortOrder = reader.GetInt32(4);
                }

            return item;
        }

        public IEnumerable<MediaStream> GetMediaStreams(MediaStreamQuery query)
        {
            CheckDisposed();

            if (query == null)
                {
                    throw new ArgumentNullException("query");
                }

            var list = new List<MediaStream>();

            using (var cmd = _connection.CreateCommand())
                {
                    var cmdText = "SELECT " + string.Join(",", _mediaStreamSaveColumns) + " FROM mediastreams WHERE";

                    cmdText += " ItemId = @ItemId";
                    cmd.Parameters.Add(cmd, "@ItemId", DbType.Guid).Value = query.ItemId;

                    if (query.Type.HasValue)
                        {
                            cmdText += " AND StreamType = @StreamType";
                            cmd.Parameters.Add(cmd, "@StreamType", DbType.String).Value = query.Type.Value.ToString();
                        }

                    if (query.Index.HasValue)
                        {
                            cmdText += " AND StreamIndex = @StreamIndex";
                            cmd.Parameters.Add(cmd, "@StreamIndex", DbType.Int32).Value = query.Index.Value;
                        }

                    cmdText += " ORDER BY StreamIndex ASC";

                    cmd.CommandText = cmdText;

                    using (var reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult))
                        {
                            while (reader.Read())
                                {
                                    list.Add(GetMediaStream(reader));
                                }
                        }
                }

            return list;
        }

        public async Task SaveMediaStreams(Guid id, List<MediaStream> streams, CancellationToken cancellationToken)
        {
            CheckDisposed();

            if (id == Guid.Empty)
                {
                    throw new ArgumentNullException("id");
                }

            if (streams == null)
                {
                    throw new ArgumentNullException("streams");
                }

            cancellationToken.ThrowIfCancellationRequested();

            await WriteLock.WaitAsync(cancellationToken).ConfigureAwait(false);

            IDbTransaction transaction = null;

            try
                {
                    transaction = _connection.BeginTransaction();

                    // First delete chapters
                    _deleteStreamsCommand.GetParameter(0).Value = id;

                    _deleteStreamsCommand.Transaction = transaction;

                    _deleteStreamsCommand.ExecuteNonQuery();

                    foreach (var stream in streams)
                        {
                            cancellationToken.ThrowIfCancellationRequested();

                            var index = 0;

                            _saveStreamCommand.GetParameter(index++).Value = id;
                            _saveStreamCommand.GetParameter(index++).Value = stream.Index;
                            _saveStreamCommand.GetParameter(index++).Value = stream.Type.ToString();
                            _saveStreamCommand.GetParameter(index++).Value = stream.Codec;
                            _saveStreamCommand.GetParameter(index++).Value = stream.Language;
                            _saveStreamCommand.GetParameter(index++).Value = stream.ChannelLayout;
                            _saveStreamCommand.GetParameter(index++).Value = stream.Profile;
                            _saveStreamCommand.GetParameter(index++).Value = stream.AspectRatio;
                            _saveStreamCommand.GetParameter(index++).Value = stream.Path;

                            _saveStreamCommand.GetParameter(index++).Value = stream.IsInterlaced;

                            _saveStreamCommand.GetParameter(index++).Value = stream.BitRate;
                            _saveStreamCommand.GetParameter(index++).Value = stream.Channels;
                            _saveStreamCommand.GetParameter(index++).Value = stream.SampleRate;

                            _saveStreamCommand.GetParameter(index++).Value = stream.IsDefault;
                            _saveStreamCommand.GetParameter(index++).Value = stream.IsForced;
                            _saveStreamCommand.GetParameter(index++).Value = stream.IsExternal;

                            _saveStreamCommand.GetParameter(index++).Value = stream.Width;
                            _saveStreamCommand.GetParameter(index++).Value = stream.Height;
                            _saveStreamCommand.GetParameter(index++).Value = stream.AverageFrameRate;
                            _saveStreamCommand.GetParameter(index++).Value = stream.RealFrameRate;
                            _saveStreamCommand.GetParameter(index++).Value = stream.Level;
                            _saveStreamCommand.GetParameter(index++).Value = stream.PixelFormat;
                            _saveStreamCommand.GetParameter(index++).Value = stream.BitDepth;
                            _saveStreamCommand.GetParameter(index++).Value = stream.IsAnamorphic;
                            _saveStreamCommand.GetParameter(index++).Value = stream.RefFrames;

                            _saveStreamCommand.GetParameter(index++).Value = stream.CodecTag;
                            _saveStreamCommand.GetParameter(index++).Value = stream.Comment;
                            _saveStreamCommand.GetParameter(index++).Value = stream.NalLengthSize;
                            _saveStreamCommand.GetParameter(index++).Value = stream.IsAVC;
                            _saveStreamCommand.GetParameter(index++).Value = stream.Title;

                            _saveStreamCommand.GetParameter(index++).Value = stream.TimeBase;
                            _saveStreamCommand.GetParameter(index++).Value = stream.CodecTimeBase;

                            _saveStreamCommand.Transaction = transaction;
                            _saveStreamCommand.ExecuteNonQuery();
                        }

                    transaction.Commit();
                } catch (OperationCanceledException)
                {
                    if (transaction != null)
                        {
                            transaction.Rollback();
                        }

                    throw;
                } catch (Exception e)
                {
                    Logger.ErrorException("Failed to save media streams:", e);

                    if (transaction != null)
                        {
                            transaction.Rollback();
                        }

                    throw;
                } finally
                {
                    if (transaction != null)
                        {
                            transaction.Dispose();
                        }

                    WriteLock.Release();
                }
        }

        /// <summary>
        /// Gets the chapter.
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <returns>ChapterInfo.</returns>
        private MediaStream GetMediaStream(IDataReader reader)
        {
            var item = new MediaStream {
                Index = reader.GetInt32(1)
            };

            item.Type = (MediaStreamType)Enum.Parse(typeof(MediaStreamType), reader.GetString(2), true);

            if (!reader.IsDBNull(3))
                {
                    item.Codec = reader.GetString(3);
                }

            if (!reader.IsDBNull(4))
                {
                    item.Language = reader.GetString(4);
                }

            if (!reader.IsDBNull(5))
                {
                    item.ChannelLayout = reader.GetString(5);
                }

            if (!reader.IsDBNull(6))
                {
                    item.Profile = reader.GetString(6);
                }

            if (!reader.IsDBNull(7))
                {
                    item.AspectRatio = reader.GetString(7);
                }

            if (!reader.IsDBNull(8))
                {
                    item.Path = reader.GetString(8);
                }

            item.IsInterlaced = reader.GetBoolean(9);

            if (!reader.IsDBNull(10))
                {
                    item.BitRate = reader.GetInt32(10);
                }

            if (!reader.IsDBNull(11))
                {
                    item.Channels = reader.GetInt32(11);
                }

            if (!reader.IsDBNull(12))
                {
                    item.SampleRate = reader.GetInt32(12);
                }

            item.IsDefault = reader.GetBoolean(13);
            item.IsForced = reader.GetBoolean(14);
            item.IsExternal = reader.GetBoolean(15);

            if (!reader.IsDBNull(16))
                {
                    item.Width = reader.GetInt32(16);
                }

            if (!reader.IsDBNull(17))
                {
                    item.Height = reader.GetInt32(17);
                }

            if (!reader.IsDBNull(18))
                {
                    item.AverageFrameRate = reader.GetFloat(18);
                }

            if (!reader.IsDBNull(19))
                {
                    item.RealFrameRate = reader.GetFloat(19);
                }

            if (!reader.IsDBNull(20))
                {
                    item.Level = reader.GetFloat(20);
                }

            if (!reader.IsDBNull(21))
                {
                    item.PixelFormat = reader.GetString(21);
                }

            if (!reader.IsDBNull(22))
                {
                    item.BitDepth = reader.GetInt32(22);
                }

            if (!reader.IsDBNull(23))
                {
                    item.IsAnamorphic = reader.GetBoolean(23);
                }

            if (!reader.IsDBNull(24))
                {
                    item.RefFrames = reader.GetInt32(24);
                }

            if (!reader.IsDBNull(25))
                {
                    item.CodecTag = reader.GetString(25);
                }

            if (!reader.IsDBNull(26))
                {
                    item.Comment = reader.GetString(26);
                }

            if (!reader.IsDBNull(27))
                {
                    item.NalLengthSize = reader.GetString(27);
                }

            if (!reader.IsDBNull(28))
                {
                    item.IsAVC = reader.GetBoolean(28);
                }

            if (!reader.IsDBNull(29))
                {
                    item.Title = reader.GetString(29);
                }

            if (!reader.IsDBNull(30))
                {
                    item.TimeBase = reader.GetString(30);
                }

            if (!reader.IsDBNull(31))
                {
                    item.CodecTimeBase = reader.GetString(31);
                }

            return item;
        }

    }
}
