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

        public AsyncRepository(Func<TAggregateRoot> rootFactory, ConcurrentUnitOfWork unitOfWork, IStreamStore eventStore, IEventDeserializer deserializer)
        {
            _rootFactory = rootFactory ?? throw new ArgumentNullException(nameof(rootFactory));
            _unitOfWork = unitOfWork ?? throw new ArgumentNullException(nameof(unitOfWork));
            _eventStore = eventStore ?? throw new ArgumentNullException(nameof(eventStore));
            _deserializer = deserializer ?? throw new ArgumentNullException(nameof(deserializer));
            _unitOfWork = unitOfWork;
            _eventStore = eventStore;
            _deserializer = deserializer; // TODO: unit test for null argument
        }

        public Func<TAggregateRoot> RootFactory
        {
            get { return _rootFactory; }
        }

        public ConcurrentUnitOfWork UnitOfWork
        {
            get { return _unitOfWork; }
        }

        public IStreamStore EventStore
        {
            get { return _eventStore; }
        }

        public IEventDeserializer EventDeserializer
        {
            get { return _deserializer; }
        }

        public async Task<TAggregateRoot> GetAsync(string identifier)
        {
            var result = await GetOptionalAsync(identifier);
            if (!result.HasValue)
                throw new AggregateNotFoundException(identifier, typeof(TAggregateRoot));
            return result.Value;
        }

        public async Task<Optional<TAggregateRoot>> GetOptionalAsync(string identifier)
        {
            Aggregate aggregate;

            if (_unitOfWork.TryGet(identifier, out aggregate))
            {
                return new Optional<TAggregateRoot>((TAggregateRoot)aggregate.Root);
            }

            var start = 0;
            const int BatchSize = 500; // TODO: configurable in ReaderConfiguration

            ReadStreamPage page;
            var events = new List<StreamMessage>();

            do
            {
                page = await _eventStore.ReadStreamForwards(identifier, start, BatchSize);

                if (page.Status == PageReadStatus.StreamNotFound)
                {
                    return Optional<TAggregateRoot>.Empty;
                }

                events.AddRange(
                    page.Messages);

                start = page.NextStreamVersion;
            }
            while (!page.IsEnd);

            var deserializedEvents = await Task.WhenAll(events.Select(resolvedMsg => _deserializer.DeserializeAsync(resolvedMsg)).ToArray());

            var root = _rootFactory();
            root.Initialize(deserializedEvents);
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
