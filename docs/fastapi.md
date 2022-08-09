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
from typing import ClassVar, Optional, List

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
    init_neontology(
        init_neontology(
        neo4j_uri="NEO4J URI HERE",
        neo4j_username="NEO4J USERNAME HERE",
        neo4j_password="NEO4J PASSWORD HERE"
        )
    )

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

`/teams/` will provide a list of all the created teams.

`/teams/<teamname>` will let us access information about a specific team.

```python
@app.get("/teams/")
async def get_teams() -> List[TeamNode]:

    return TeamNode.match_nodes()


@app.get("/teams/{pp}")
async def get_team(pp: str) -> Optional[TeamNode]:

    return TeamNode.match(pp)

```

You can now browse the API to create teams and get info about them.

## Finishing Up

We can then add some more routes which will let us create team members and assign them to teams.

In full, this becomes:

```python
# main.py
from typing import ClassVar, Optional, List

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
    init_neontology()


@app.get("/")
def read_root():
    return {"foo": "bar"}


@app.post("/teams/")
async def create_team(team: TeamNode):

    team.create()

    return team


@app.get("/teams/")
async def get_teams() -> List[TeamNode]:

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
async def get_team_members() -> List[TeamMemberNode]:

    return TeamMemberNode.match_nodes()


@app.get("/team-members/{pp}")
async def get_team_member(pp: str) -> Optional[TeamMemberNode]:

    return TeamMemberNode.match(pp)
```
