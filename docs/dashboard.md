# Octoflow Dashboard

The dashboard is a single static page, `web/index.html`, served by
`octoflow dashboard`. It connects to the SSE server with the browser's
EventSource and shows events as they arrive.

## Features

### Multiple Subscription Patterns

The dashboard supports subscribing to different event streams:

- **All Events (Firehose)** - See all events across the organization
- **Organization Events** - Filter by specific organization ID
- **Repository Events** - Monitor a specific repository by ID
- **User Activity** - Track all events from a specific user
- **Pull Request Events** - Focus on a specific PR
- **Issue Events** - Monitor a specific issue

### Real-time Event Display

- Live streaming of GitHub events as they occur
- Color-coded event type badges
- Event details including repository, user, and action
- Automatic parsing of PR/Issue titles and states
- Animation effects for new events

### Statistics

- Total events received
- Events per minute rate
- Connected clients count
- Active stream indicator

### Filtering

- Filter by event types (e.g., `push,pull_request`)
- Filter by actions (e.g., `opened,closed,merged`)
- Filters can be combined with any subscription pattern

## Usage

### Starting the Dashboard

1. Start the services:
   ```bash
   make run
   ```

2. Open http://localhost:8082

3. The page connects to the firehose (all events) on load

### Finding the SSE server

The page finds the SSE server from its own address: port 8081 on the same
host when the page is served from port 8082, otherwise the page's own
origin (the NixOS module serves both behind nginx). `?sse=https://host`
overrides it.

### Automatic Name to ID Resolution

The dashboard now automatically converts GitHub names to IDs:

- **Organizations**: Type the org name (e.g., "octocat") and the dashboard will look up the ID
- **Repositories**: Type "owner/repo" format and get the ID automatically
- **Users**: Type the username and the ID is resolved for you

You can also directly enter numeric IDs if you already know them. The dashboard will detect numeric input and use it as-is.

### Subscription Examples

1. **Monitor a specific repository**:
   - Select "Repository Events"
   - Enter the repository ID (e.g., `123456789`)
   - Click "Apply & Connect"

2. **Track PR activity**:
   - Select "Pull Request Events"
   - Enter the repository ID and PR number
   - Click "Apply & Connect"

3. **Filter push events only**:
   - In the "Event Types" filter, enter `push`
   - Click "Apply & Connect"

## Architecture

The dashboard consists of:

1. **Frontend (index.html)**:
   - Pure JavaScript with EventSource API
   - No build process required
   - Responsive design with CSS Grid

2. **SSE server (`octoflow sse`, port 8081)**:
   - One route per subscription pattern, see `api/openapi-sse-example.yaml`
   - Replays the last events, resumes with `Last-Event-ID`

3. **Dashboard server (`octoflow dashboard`, port 8082)**:
   - Serves `web/`; `DASHBOARD_LISTEN_ADDR` and `WEB_DIR` change the defaults

## Event Types

The dashboard recognizes and color-codes these event types:

- `pull_request` - Purple
- `issues` - Red
- `push` - Green
- `release` - Blue
- `create` - Teal
- `delete` - Orange
- `star` - Yellow
- `fork` - Light Purple
- And many more...

## Troubleshooting

### No events appearing

1. Check that all services are running:
   ```bash
   make run
   ```

2. Verify NATS connection in SSE server logs

3. Ensure GitHub webhooks are configured and delivering

### Connection errors

1. Check the browser console for the SSE URL it tried
2. Verify the SSE server answers: `curl -N http://localhost:8081/events?last=1`
3. If the SSE server is elsewhere, open the page with `?sse=<url>`

### Performance

- Events are limited to 100 in the display
- Older events are automatically removed
- Stats update every second
- Server stats refresh every 5 seconds