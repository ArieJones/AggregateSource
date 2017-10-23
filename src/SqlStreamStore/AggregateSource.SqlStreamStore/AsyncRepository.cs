using SqlStreamStore;
using SqlStreamStore.Streams;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AggregateSource.SqlStreamStore
{
    public class AsyncRepository<TAggregateRoot> : IAsyncRepository<TAggregateRoot>
        where TAggregateRoot : IAggregateRootEntity
    {
        private readonly Func<TAggregateRoot> _rootFactory;
        private readonly ConcurrentUnitOfWork _unitOfWork;
        private readonly IStreamStore _eventStore;
        private readonly IEventDeserializer _deserializer;
        private readonly int _pageSize;

        public AsyncRepository(Func<TAggregateRoot> rootFactory, ConcurrentUnitOfWork unitOfWork, IStreamStore eventStore, IEventDeserializer deserializer)
        {
            _rootFactory = rootFactory ?? throw new ArgumentNullException(nameof(rootFactory));
            _unitOfWork = unitOfWork ?? throw new ArgumentNullException(nameof(unitOfWork));
            _eventStore = eventStore ?? throw new ArgumentNullException(nameof(eventStore));
            _deserializer = deserializer ?? throw new ArgumentNullException(nameof(deserializer));
            _rootFactory = rootFactory;
            _unitOfWork = unitOfWork;
            _eventStore = eventStore;
            _deserializer = deserializer;
            _pageSize = 500;
        }

        public Func<TAggregateRoot> RootFactory => _rootFactory;
        public ConcurrentUnitOfWork UnitOfWork => _unitOfWork;
        public IStreamStore EventStore => _eventStore;
        public IEventDeserializer EventDeserializer => _deserializer;

        public async Task<TAggregateRoot> GetAsync(string identifier)
        {
            var result = await GetOptionalAsync(identifier).ConfigureAwait(false);
            if (!result.HasValue)
                throw new AggregateNotFoundException(identifier, typeof(TAggregateRoot));
            return result.Value;
        }

        public async Task<Optional<TAggregateRoot>> GetOptionalAsync(string identifier)
        {
            if (_unitOfWork.TryGet(identifier, out var aggregate))
                return new Optional<TAggregateRoot>((TAggregateRoot)aggregate.Root);

            var rawEvents = new List<StreamMessage>();
            var page = await _eventStore.ReadStreamForwards(identifier, StreamVersion.Start, _pageSize).ConfigureAwait(false);
            do
            {
                if (page.Status == PageReadStatus.StreamNotFound)
                    return Optional<TAggregateRoot>.Empty;
                rawEvents.AddRange(page.Messages);
            } while (!page.IsEnd);

            var events = await Task.WhenAll(rawEvents.Select(resolvedMsg => _deserializer.DeserializeAsync(resolvedMsg))).ConfigureAwait(false);

            var root = _rootFactory();
            root.Initialize(events);
            aggregate = new Aggregate(identifier, page.LastStreamVersion, root);
            _unitOfWork.Attach(aggregate);

            return new Optional<TAggregateRoot>(root);
        }

        public void Add(string identifier, TAggregateRoot root)
        {
            _unitOfWork.Attach(new Aggregate(identifier, ExpectedVersion.NoStream, root));
        }
    }
}
