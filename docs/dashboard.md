# Octoflow Dashboard

The Octoflow Dashboard provides a real-time visualization of GitHub events flowing through your system. It connects to the enhanced SSE server to display events with powerful filtering and subscription capabilities.

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

1. Ensure the SSE server is running on port 8081:
   ```bash
   make run
   ```

2. Access the dashboard at http://localhost:8082

3. The dashboard will automatically connect to the firehose (all events) on load

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

2. **SSE Server (port 8081)**:
   - Multiple endpoint support
   - Per-stream subscriptions
   - Real-time event streaming

3. **Dashboard Server (port 8082)**:
   - Simple static file server
   - Serves the HTML/CSS/JS files

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

1. Check browser console for errors
2. Verify SSE server is running on port 8081
3. Check for CORS issues if accessing from different domain

### Performance

- Events are limited to 100 in the display
- Older events are automatically removed
- Stats update every second
- Server stats refresh every 5 seconds