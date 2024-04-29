using Marten;
using Marten.Events;
using Marten.Events.Aggregation;
using Marten.Events.Projections;
using static MartenFanOutRepro.Tests.TestDatabase;

namespace MartenFanOutRepro.Tests;

public record IncidentLogged(DateTimeOffset On);

public record Incident(DateTimeOffset On)
{
  public Guid Id { get; set; }
};

public record ChatStarted(Guid ContextId, DateTimeOffset On);

public record Chat(string ContextId)
{
  public string Id { get; set; }
};

public class IncidentChatProjection : MultiStreamProjection<Chat, string>
{
  public IncidentChatProjection()
  {
    FanOut<IncidentLogged, ChatStarted>(
      e => new[]
      {
        new ChatStarted(
          e.StreamId,
          e.Data.On
        )
      },
      FanoutMode.BeforeGrouping
    );
    Identity<ChatStarted>(e => $"incident-chat-{e.ContextId}");
    IncludeType<IncidentLogged>();
  }

  public static Chat Create(
    IEvent<ChatStarted> @event
  ) => new(
    @event.StreamKey
  )
  {
    Id = @event.StreamKey
  };
}

public class IncidentProjection : SingleStreamProjection<Incident>
{
  public static Incident Create(
    IncidentLogged logged
  ) =>
    new(logged.On);
}

[TestFixture]
public class When_incident_is_logged
{
  private DocumentStore? _store;
  private Guid _streamId;

  [SetUp]
  public async Task Setup()
  {
    _store = DocumentStore.For(
      _ =>
      {
        _.Connection(GetTestDbConnectionString());
        _.Projections.Add<IncidentProjection>(ProjectionLifecycle.Inline);
        _.Projections.Add<IncidentChatProjection>(ProjectionLifecycle.Inline);
      }
    );
    var on = DateTimeOffset.Now;
    var logged = new IncidentLogged(on);
    _streamId = Guid.NewGuid();

    await using var session = _store?.LightweightSession();
    session?.Events.Append(_streamId, logged);
    await session?.SaveChangesAsync()!;
  }

  [Test]
  public async Task should_create_incident_projection()
  {
    await using var querySession = _store?.QuerySession();
    var incident = await querySession?.LoadAsync<Incident>(_streamId)!;
    incident.ShouldNotBeNull();
  }

  [Test]
  public async Task should_create_chat_projection()
  {
    await using var querySession = _store?.QuerySession();
    var chat = await querySession?.Query<Chat>().ToListAsync()!;
    chat.Count.ShouldBeGreaterThan(0);
  }
}
