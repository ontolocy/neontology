"""One connection shared by several threads, as a web server serving requests does.

Every engine is used this way, so every engine has to stay correct under it. Ladybug runs
the queries one at a time on its single connection; the Bolt drivers run them in parallel.
Each thread writes nodes of its own: two threads merging the same node is only guaranteed
to give one node where a uniqueness constraint backs it, on the servers.
"""

from concurrent.futures import ThreadPoolExecutor
from typing import ClassVar

from neontology import BaseNode, BaseRelationship

THREADS = 8
NODES_PER_THREAD = 25


class ConcurrentNode(BaseNode):
    __primarylabel__: ClassVar[str] = "ConcurrentNode"
    __primaryproperty__: ClassVar[str] = "pp"

    pp: str


class ConcurrentLink(BaseRelationship):
    __relationshiptype__: ClassVar[str] = "CONCURRENT_LINK"

    source: ConcurrentNode
    target: ConcurrentNode


def _write_a_chain(thread: int) -> None:
    """Merge a chain of nodes, reading each back as it goes."""
    for i in range(NODES_PER_THREAD):
        ConcurrentNode(pp=f"t{thread}-{i}").merge()

        if i:
            ConcurrentLink(source=ConcurrentNode(pp=f"t{thread}-{i - 1}"), target=ConcurrentNode(pp=f"t{thread}-{i}")).merge()

        assert ConcurrentNode.match(f"t{thread}-{i}") is not None


def test_threads_sharing_the_connection_merge_and_match(use_graph):
    with ThreadPoolExecutor(THREADS) as pool:
        # list() so an exception raised in any thread is raised here
        list(pool.map(_write_a_chain, range(THREADS)))

    assert ConcurrentNode.get_count() == THREADS * NODES_PER_THREAD
    assert ConcurrentLink.get_count() == THREADS * (NODES_PER_THREAD - 1)
