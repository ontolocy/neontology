# Neo4j and FastAPI Tutorial

FastAPI is a fantastic library for building APIs with Python. From the [documentation](https://fastapi.tiangolo.com/):

> FastAPI is a modern, fast (high-performance), web framework for building APIs with Python 3.6+ based on standard Python type hints.

Because both FastAPI and Neontology utilise Pydantic models, we can combine them to quickly build a Python based Neo4j API.

We'll need a few things before we begin:

```bash
pip install fastapi
pip install "uvicorn[standard]"
```

You'll also need a live Neo4j instance to connect to.

## Defining Models

We're going to build a limited API for managing teams and team members.

First we need to define models for the different types of node we want to store.

```python
# main.py
from typing import ClassVar, Optional

from fastapi import FastAPI, HTTPException
from neontology import BaseNode, BaseRelationship, init_neontology, Neo4jConfig

class TeamNode(BaseNode):
    __primaryproperty__: ClassVar[str] = "teamname"
    __primarylabel__: ClassVar[str] = "Team"
    teamname: str
    slogan: str = "Better than the rest!"


class TeamMemberNode(BaseNode):
    __primaryproperty__: ClassVar[str] = "nickname"
    __primarylabel__: ClassVar[str] = "TeamMember"
    nickname: str
```

## Adding a relationship

Team members should 'belong to' a team, so lets define that relationship.

```python
class BelongsTo(BaseRelationship):
    __relationshiptype__: ClassVar[str] = "BELONGS_TO"

    source: TeamMemberNode
    target: TeamNode
```

## Our first root

Now we need to initialise FastAPI and Neontology, and let's create a root route to try things:

```python
app = FastAPI()

@app.on_event("startup")
async def startup_event():

    # here we declare the neo4j connection details explicitly (this can be bad for security)
    # you could instead define them as environment variables or in a .env file
    NEO4J_URI="neo4j+s://<database id>.databases.neo4j.io"  # neo4j Aura example
    NEO4J_USERNAME="neo4j"
    NEO4J_PASSWORD="<your password>"

    config = Neo4jConfig(
        uri=NEO4J_URI,
        username=NEO4J_USERNAME,
        password=NEO4J_PASSWORD
    )
    init_neontology(config)


@app.get("/")
def read_root():
    return {"foo": "bar"}
```

## What we have so far

With all this defined in `main.py`, we can check it's running with:

```bash
uvicorn main:app --reload
```

Now if you go to `http://127.0.0.1:8000/` you should simply get `{"foo":"bar"}`.

But for a much more comprehensive view, you can checkout `http://127.0.0.1:8000/docs` or `http://127.0.0.1:8000/redoc`.

## Adding teams

Now lets start actually interacting with our Neo4j graph.

We'll define a `POST` rout for adding a new team.

Because FastAPI and Neontology both use Pydantic models, we just need to give our node type as a type hint on the route so that FastAPI knows that data it receives should map to the TeamNode type.

Then we use Neontology's `create()` method to create that team in the database.

```python
@app.post("/teams/")
async def create_team(team: TeamNode):

    team.create()

    return team
```

Now visit `http://127.0.0.1:8000/docs` again where you should see our `POST` entry for `/teams/`. Explore the information and then hit the `Try it out` button to post some information and create a team.

## Getting teams

Here we'll add some more routes to get the teams that have been created.

### Getting all teams

`/teams/` will provide a list of all the created teams.

```python
@app.get("/teams/")
async def get_teams() -> list[TeamNode]:
    return TeamNode.match_nodes()
```

### Getting a team based on its primary property

`/teams/<teamname>` will let us access information about a specific team.

```python
@app.get("/teams/{pp}")
async def get_team(pp: str) -> Optional[TeamNode]:

    return TeamNode.match(pp)
```

### Getting teams based on filtering

You can also retrieve teams based on various filter criteria. The `match_nodes` method supports a wide range of filter options using a Django-like syntax. Here are some examples of how you can use filtering:

#### Basic filtering

To get all teams with a specific slogan:

```python
TeamNode.match_nodes(filters={"slogan": "Better than the rest!"})
```

#### String-based filters

- `icontains`: Case-insensitive contains
  ```python
  TeamNode.match_nodes(filters={"teamname__icontains": "team"})
  ```
- `contains`: Case-sensitive contains
  ```python
  TeamNode.match_nodes(filters={"teamname__contains": "Team"})
  ```
- `iexact`: Case-insensitive exact match
  ```python
  TeamNode.match_nodes(filters={"teamname__iexact": "team a"})
  ```
- `startswith`: Case-sensitive startswith
  ```python
  TeamNode.match_nodes(filters={"teamname__startswith": "Tea"})
  ```
- `istartswith`: Case-insensitive startswith
  ```python
  TeamNode.match_nodes(filters={"teamname__istartswith": "tea"})
  ```

#### Numeric filters

For numeric fields (if any were present), you could use:

- `gt`: Greater than
- `lt`: Less than
- `gte`: Greater than or equal to
- `lte`: Less than or equal to

#### Boolean filters

For boolean fields, you can simply use the field name with the desired boolean value:

```python
TeamNode.match_nodes(filters={"is_active": True})
```

#### Null checks

To filter based on null values:

```python
TeamNode.match_nodes(filters={"slogan__isnull": True})  # Teams with no slogan
```

#### Combining filters

You can combine multiple filters to create more complex queries:

```python
TeamNode.match_nodes(filters={
    "slogan__icontains": "better",
    "teamname__startswith": "A"
})
```

### Pagination

You can also paginate results using `limit` and `skip` parameters:

```python
TeamNode.match_nodes(limit=10, skip=20)  # Get 10 teams, skipping the first 20
```

These filtering capabilities provide a powerful way to query your data with flexibility and precision.

You can now browse the API to create teams and get info about them.

## Finishing Up

We can then add some more routes which will let us create team members and assign them to teams.

In full, this becomes:

```python
# main.py
from typing import ClassVar, Optional

from fastapi import FastAPI, HTTPException
from neontology import BaseNode, BaseRelationship, init_neontology


class TeamNode(BaseNode):
    __primaryproperty__: ClassVar[str] = "teamname"
    __primarylabel__: ClassVar[str] = "Team"
    teamname: str
    slogan: str = "Better than the rest!"


class TeamMemberNode(BaseNode):
    __primaryproperty__: ClassVar[str] = "nickname"
    __primarylabel__: ClassVar[str] = "TeamMember"
    nickname: str


class BelongsTo(BaseRelationship):
    __relationshiptype__: ClassVar[str] = "BELONGS_TO"

    source: TeamMemberNode
    target: TeamNode


app = FastAPI()


@app.on_event("startup")
async def startup_event():
    # make sure you've set NEO4J_URI, NEO4J_USERNAME and NEO4J_PASSWORD environment variables
    # they could be defined in a .env file
    init_neontology()


@app.get("/")
def read_root():
    return {"foo": "bar"}


@app.post("/teams/")
async def create_team(team: TeamNode):

    team.create()

    return team


@app.get("/teams/")
async def get_teams() -> list[TeamNode]:

    return TeamNode.match_nodes()


@app.get("/teams/{pp}")
async def get_team(pp: str) -> Optional[TeamNode]:

    return TeamNode.match(pp)


@app.post("/team-members/")
async def create_team_member(member: TeamMemberNode, team_name: str):

    team = TeamNode.match(team_name)

    if team is None:
        raise HTTPException(status_code=404, detail="Team doesn't exist")

    member.create()

    rel = BelongsTo(source=member, target=team)
    rel.merge()

    return member


@app.get("/team-members/")
async def get_team_members() -> list[TeamMemberNode]:

    return TeamMemberNode.match_nodes()


@app.get("/team-members/{pp}")
async def get_team_member(pp: str) -> Optional[TeamMemberNode]:

    return TeamMemberNode.match(pp)
```
